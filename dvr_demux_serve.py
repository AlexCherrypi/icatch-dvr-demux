#!/usr/bin/env python3
# DVR Demux Service for Vantech iWatch DVR (iCatch SoC, MayaRobot custom format).
#
# Pulls one or more multipart-mixed-replace streams from the DVR's net_video.cgi
# endpoint, parses the proprietary chunk format, and re-emits per-camera
# H.264 streams over HTTP for downstream consumption by go2rtc / ffmpeg / Frigate.
#
# Architecture:
#   DVR (HTTP CGI, multipart) -> demuxer -> 11x raw-H.264 HTTP outputs
#                                           (localhost:8557/cam<N>)
#
# Format reverse-engineered from MoaiRobot.dylib RemoteStream::{Start,Split}
# (the dynamically loaded body of the vendor's iWatchDVR client; the
# matching field layout is also documented in this repo's README):
#
#   - HTTP multipart-mixed-replace with --myboundary
#   - Each multipart section has standard headers, blank line, then a binary
#     body starting with magic 0x00001234.
#   - Top-level binary header (288 bytes from magic to first sub-chunk):
#       +0x00 magic         (uint32 LE = 0x00001234)
#       +0x0c ts1           (uint32, "from-time" or capture timestamp)
#       +0x10 ts2           (uint32, "to-time" or session-relative)
#       +0x1c chunk_count   (uint32, number of sub-chunks following)
#       +0x120 first sub-chunk header
#   - Each sub-chunk is a 44-byte (0x2c) header + variable payload:
#       +0x00 type      (uint32: 0=I-frame, 1=P-frame, 2=audio,
#                                3/4=skip, 5=AVD, 6=PEA, 7=OSC)
#       +0x04 channel   (uint32, cam index 0..10)
#       +0x08 width     (uint32)
#       +0x0c height    (uint32)
#       +0x24 size      (uint32, payload size)
#       +0x28 next      (uint32, byte offset from end of this header to start
#                         of next sub-chunk header; usually == size)
#   - CGI URL parameter order is exactly:
#       /cgi-bin/net_video.cgi
#         ?hq=%d&iframe=%d&pframe=%d&audio=%d&complete=%d&beg=%d&end=%d&ivs=%d
#     The original MayaRobot client always sends complete=0 in live mode and
#     never sets ivs (=0) unless IVS overlays are requested. We mirror that.

import argparse
import collections
import http.server
import json
import logging
import queue
import socketserver
import struct
import threading
import time
from collections import defaultdict
from urllib.parse import urlparse, urlsplit, urlunsplit

import requests


MAGIC_WORD = 0x00001234
BOUNDARY = b'--myboundary'
NUM_CAMERAS = 11

# Top-level header offsets (within the multipart body, starting at magic).
HDR_MAGIC       = 0x00
HDR_TS1         = 0x0c  # frame timestamp from iCatch encoder
HDR_TS2         = 0x10  # secondary timestamp
HDR_COUNT       = 0x1c  # number of sub-chunks
HDR_SUB_OFFSET  = 0x120 # offset of first sub-chunk header from start of body

# Sub-chunk header layout (44 bytes).
SUB_HEADER_SIZE = 0x2c
SUB_TYPE        = 0x00
SUB_CHANNEL     = 0x04
SUB_WIDTH       = 0x08
SUB_HEIGHT      = 0x0c
SUB_SIZE        = 0x24
SUB_NEXT_STEP   = 0x28  # bytes from sub-payload start to next sub-chunk header

# Sub-chunk types (from RemoteStream::Split switch statement).
SUB_TYPE_IFRAME = 0
SUB_TYPE_PFRAME = 1
SUB_TYPE_AUDIO  = 2
# 3, 4 are explicit no-ops in the original. 5/6/7 are IVS metadata
# (motion detection, line crossing, scene change) -- we suppress via ivs=0
# in the URL and ignore here defensively.
SUB_TYPE_AVD    = 5
SUB_TYPE_PEA    = 6
SUB_TYPE_OSC    = 7

VIDEO_TYPES = {SUB_TYPE_IFRAME, SUB_TYPE_PFRAME}

log = logging.getLogger('dvr-demux')


# Observability-only: bucket per-cam puller-side inter-arrival times.
# Tells us whether the DVR delivers frames evenly or bursty per channel.
_INTER_ARRIVAL_BUCKETS = (
    (50,    '<50ms'),
    (200,   '50-200ms'),
    (500,   '200-500ms'),
    (1000,  '500ms-1s'),
    (2000,  '1-2s'),
    (5000,  '2-5s'),
    (10000, '5-10s'),
)


def _bucket(delta_ms):
    for limit, label in _INTER_ARRIVAL_BUCKETS:
        if delta_ms < limit:
            return label
    return '>10s'


def parse_chunk_header(buf):
    """Validate magic + return (ts1, ts2, chunk_count) or None."""
    if len(buf) < HDR_SUB_OFFSET:
        return None
    magic = struct.unpack_from('<I', buf, HDR_MAGIC)[0]
    if magic != MAGIC_WORD:
        return None
    return dict(
        ts1=struct.unpack_from('<I', buf, HDR_TS1)[0],
        ts2=struct.unpack_from('<I', buf, HDR_TS2)[0],
        chunk_count=struct.unpack_from('<I', buf, HDR_COUNT)[0],
    )


def iterate_sub_chunks(buf, count):
    """Yield (sub_type, channel, payload_bytes) per sub-chunk.

    Mirrors RemoteStream::Split iteration: starts at HDR_SUB_OFFSET,
    advances by SUB_HEADER_SIZE + next_step (read from header+0x28).
    """
    off = HDR_SUB_OFFSET
    buf_len = len(buf)
    for _ in range(count):
        if off + SUB_HEADER_SIZE > buf_len:
            return
        sub_type = struct.unpack_from('<I', buf, off + SUB_TYPE)[0]
        channel  = struct.unpack_from('<I', buf, off + SUB_CHANNEL)[0]
        size     = struct.unpack_from('<I', buf, off + SUB_SIZE)[0]
        next_step = struct.unpack_from('<I', buf, off + SUB_NEXT_STEP)[0]
        body_start = off + SUB_HEADER_SIZE
        body_end = min(body_start + size, buf_len)
        yield sub_type, channel, bytes(buf[body_start:body_end])
        off = body_start + next_step


class MultipartStreamParser:
    """Splits a multipart/x-mixed-replace stream and emits the binary body
    of each multipart section to the callback as one bytes() buffer."""

    def __init__(self, boundary=BOUNDARY, callback=None):
        self.boundary = boundary
        self.callback = callback
        self.buf = bytearray()

    def feed(self, data):
        self.buf.extend(data)
        while True:
            b0 = self.buf.find(self.boundary)
            if b0 == -1:
                if len(self.buf) > 64:
                    del self.buf[:len(self.buf) - 64]
                return
            ct_end = self.buf.find(b'\r\n\r\n', b0)
            if ct_end == -1:
                return
            body_start = ct_end + 4
            b1 = self.buf.find(self.boundary, body_start)
            if b1 == -1:
                return
            chunk = bytes(self.buf[body_start:b1])
            if self.callback:
                self.callback(chunk)
            del self.buf[:b1]


def find_nal(buf, start=0):
    """Find next H.264 NAL start code; return (start_index, prefix_len, nal_type)."""
    i = buf.find(b'\x00\x00\x01', start)
    if i == -1:
        return -1, 0, -1
    prefix = 3
    if i > 0 and buf[i - 1] == 0:
        i -= 1
        prefix = 4
    if i + prefix >= len(buf):
        return -1, 0, -1
    return i, prefix, buf[i + prefix] & 0x1F


def scan_nals(buf):
    out = []
    pos = 0
    while True:
        i, p, t = find_nal(buf, pos)
        if i == -1:
            break
        out.append((i, p, t))
        pos = i + p
    return out


class FrameRingBuffer:
    """Bounded per-consumer frame buffer with I-frame-aware drop on overflow.

    Mirrors the queue-overflow strategy from VideoDispatchThread::ThreadStart
    in MoaiRobot.dylib: when the buffer hits maxsize, scan from the tail
    backwards for the latest I-frame in the buffer and drop everything
    before it. The downstream decoder can resume from that I-frame without
    "reference picture missing" errors. P-frames belonging to an already-
    decoded GOP are dropped preferentially over I-frames.
    """

    def __init__(self, maxsize=441):
        self._buf = collections.deque()
        self._cond = threading.Condition()
        self._maxsize = maxsize
        self._stopped = False
        self.dropped = 0

    def put(self, frame_bytes, is_keyframe):
        with self._cond:
            if len(self._buf) >= self._maxsize:
                # Find the latest I-frame in the buffer (scan from tail).
                last_iframe = -1
                for n in range(len(self._buf) - 1, -1, -1):
                    if self._buf[n][1]:
                        last_iframe = n
                        break
                if last_iframe > 0:
                    # Drop everything before that I-frame.
                    for _ in range(last_iframe):
                        self._buf.popleft()
                        self.dropped += 1
                else:
                    # Either no I-frame buffered, or the I-frame is already
                    # at the head (entire buffer is one GOP). Drop the
                    # oldest entry to bound memory; decoder will resync at
                    # the next I-frame.
                    self._buf.popleft()
                    self.dropped += 1
            self._buf.append((frame_bytes, is_keyframe))
            self._cond.notify()

    def get(self, timeout=None):
        """Return (frame_bytes, is_keyframe); raises queue.Empty on timeout."""
        with self._cond:
            deadline = (time.monotonic() + timeout) if timeout is not None else None
            while not self._buf and not self._stopped:
                wait_t = (deadline - time.monotonic()) if deadline is not None else None
                if wait_t is not None and wait_t <= 0:
                    raise queue.Empty
                self._cond.wait(timeout=wait_t)
            if self._stopped and not self._buf:
                raise queue.Empty
            return self._buf.popleft()

    def stop(self):
        with self._cond:
            self._stopped = True
            self._cond.notify_all()

    def qsize(self):
        with self._cond:
            return len(self._buf)


class CameraStream:
    """Per-camera input ring + pacing dispatcher + consumer fan-out.

    Mirrors VideoDispatchThread from MoaiRobot.dylib: arrival-order FIFO,
    no timestamp-based reordering, I-frame-aware drop on overflow, AND
    wallclock pacing at a fixed target rate (the original uses a
    self-generated PTS counter, equivalent for our purposes).

    Pipeline:
      puller threads -> push_frame() -> input_ring (FrameRingBuffer)
                                            |
                                            v
                                 dispatcher thread (1 per cam)
                                            |
                                            v
                                 sleep to maintain target_interval
                                            |
                                            v
                            consumer rings (one per active HTTP /cam<N>)

    The dispatcher absorbs upstream bursts (DVR delivers multiple frames
    per multipart chunk on a single TCP socket) and emits them at a steady
    rate so that downstream (go2rtc/NVENC) sees uniform inter-frame timing.
    """

    def __init__(self, cam_idx, target_fps=25.0):
        self.cam_idx = cam_idx
        self.target_interval = 1.0 / target_fps if target_fps > 0 else 0.0
        self.lock = threading.Lock()  # protects consumers + cache + stats
        self.consumers = []
        self.total_frames = 0
        self.total_bytes = 0
        self.last_frame_ts = 0.0
        self.cached_sps = None
        self.cached_pps = None
        self.cached_idr_frame = None
        # Puller-side inter-arrival histogram. Measured BEFORE the input ring
        # so it shows how the DVR delivers, not how the dispatcher emits.
        self._inter_arrival_last_ts = None
        self._inter_arrival_hist = collections.Counter()
        # Input buffer between puller and dispatcher.
        self._input_ring = FrameRingBuffer(maxsize=441)
        self._stop = threading.Event()
        self._dispatcher = threading.Thread(
            target=self._dispatch_loop,
            name=f'cam{cam_idx}-dispatcher',
            daemon=True,
        )
        self._dispatcher.start()

    def add_consumer(self, ring):
        with self.lock:
            bootstrap = b''
            if self.cached_sps:
                bootstrap += self.cached_sps
            if self.cached_pps:
                bootstrap += self.cached_pps
            if self.cached_idr_frame:
                bootstrap += self.cached_idr_frame
            if bootstrap:
                # Bootstrap is SPS+PPS+IDR -- decodable on its own == keyframe.
                ring.put(bootstrap, True)
            self.consumers.append(ring)

    def remove_consumer(self, ring):
        with self.lock:
            try:
                self.consumers.remove(ring)
            except ValueError:
                pass

    def _extract_decoder_units(self, frame_bytes):
        nals = scan_nals(frame_bytes)
        if not nals:
            return
        n = len(nals)
        idr_chunks = []
        for k in range(n):
            i, _, t = nals[k]
            j = nals[k + 1][0] if k + 1 < n else len(frame_bytes)
            piece = bytes(frame_bytes[i:j])
            if t == 7:
                self.cached_sps = piece
            elif t == 8:
                self.cached_pps = piece
            elif t == 5:
                idr_chunks.append(piece)
        if idr_chunks:
            self.cached_idr_frame = b''.join(idr_chunks)

    def push_frame(self, frame_bytes, is_keyframe):
        """Enqueue a frame for paced dispatch. Non-blocking."""
        if not frame_bytes:
            return
        now = time.monotonic()
        if self._inter_arrival_last_ts is not None:
            delta_ms = (now - self._inter_arrival_last_ts) * 1000.0
            self._inter_arrival_hist[_bucket(delta_ms)] += 1
        self._inter_arrival_last_ts = now
        self._input_ring.put(frame_bytes, is_keyframe)

    def _dispatch_loop(self):
        last_emit_at = None
        while not self._stop.is_set():
            try:
                frame, is_kf = self._input_ring.get(timeout=1.0)
            except queue.Empty:
                continue
            # Pace so successive emits are >= target_interval apart.
            # If we've drifted more than 1 s behind, drop the baseline and
            # emit now -- mirrors the +-1000 ms wait-time clamp in the
            # original ThreadStart so a long stall doesn't translate into
            # infinite catch-up bursts.
            if self.target_interval > 0 and last_emit_at is not None:
                elapsed = time.monotonic() - last_emit_at
                if elapsed < self.target_interval:
                    time.sleep(self.target_interval - elapsed)
                elif elapsed > self.target_interval + 1.0:
                    last_emit_at = None
            if self.target_interval > 0:
                last_emit_at = time.monotonic()
            with self.lock:
                self.total_frames += 1
                self.total_bytes += len(frame)
                self.last_frame_ts = time.time()
                self._extract_decoder_units(frame)
                for ring in list(self.consumers):
                    ring.put(frame, is_kf)

    def stop(self):
        self._stop.set()
        self._input_ring.stop()
        self._dispatcher.join(timeout=2)

    @property
    def input_dropped(self):
        return self._input_ring.dropped

    def consumer_dropped(self):
        with self.lock:
            return sum(r.dropped for r in self.consumers)


def _strip_query(url):
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))


class DvrGroupPuller:
    """One HTTP pull from the DVR with the iframe/pframe bitmask set for a
    GROUP of cameras. Dispatches each sub-chunk to its channel's CameraStream
    based on the channel field in the sub-header.

    Empirically (pre-rewrite), pulling cameras in groups of ~4 gives the best
    per-cam fps: solo would max ~20 fps but uses 11 connections (throttled),
    one multi-cam pull gives 5-9 fps, while 3 groups of 4+4+3 yields a
    balanced 10-14 fps per cam.
    """

    def __init__(self, base_url, target_cams, cameras_dict):
        self.target_cams = frozenset(target_cams)
        self.target_mask = sum(1 << c for c in self.target_cams)
        # URL parameter order matches MayaRobot RemoteStream::Start exactly:
        #   hq, iframe, pframe, audio, complete, beg, end, ivs
        # Live-mode values mirror the original client (branch 2):
        #   hq=1, iframe=mask, pframe=mask, audio=0, complete=0,
        #   beg=-1, end=-1, ivs=0
        self.url = (
            f"{base_url}?hq=1"
            f"&iframe={self.target_mask}"
            f"&pframe={self.target_mask}"
            f"&audio=0"
            f"&complete=0"
            f"&beg=-1"
            f"&end=-1"
            f"&ivs=0"
        )
        self.cameras = cameras_dict
        self.parser = MultipartStreamParser(callback=self._on_chunk)
        self._stop = threading.Event()
        self.total_chunks = 0
        self.total_sub_chunks = 0
        self.sub_type_counts = defaultdict(int)
        # Per-channel sub-chunk type breakdown. Counts EVERY sub-chunk the
        # parser yields, including off-target and non-video, so we can see
        # how much the DVR actually sends per cam vs what we forward.
        self.per_channel_sub_type_counts = defaultdict(lambda: defaultdict(int))
        self.dropped_off_target = 0

    def _on_chunk(self, chunk):
        self.total_chunks += 1
        hdr = parse_chunk_header(chunk)
        if hdr is None:
            return
        for sub_type, channel, payload in iterate_sub_chunks(chunk, hdr['chunk_count']):
            self.total_sub_chunks += 1
            self.sub_type_counts[sub_type] += 1
            if 0 <= channel < NUM_CAMERAS:
                self.per_channel_sub_type_counts[channel][sub_type] += 1
            if sub_type not in VIDEO_TYPES:
                # Audio (2), reserved (3/4), IVS metadata (5/6/7) -- ignore.
                # ivs=0 in the URL should keep 5/6/7 out, audio=0 keeps 2 out.
                continue
            if channel < 0 or channel >= NUM_CAMERAS:
                continue
            if channel not in self.target_cams:
                # Cam outside our group: another puller covers it.
                self.dropped_off_target += 1
                continue
            self.cameras[channel].push_frame(
                payload, is_keyframe=(sub_type == SUB_TYPE_IFRAME),
            )

    def run(self):
        log.info('GroupPuller cams=%s mask=%d url=%s',
                 sorted(self.target_cams), self.target_mask, self.url)
        while not self._stop.is_set():
            try:
                with requests.get(self.url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    log.info('GroupPuller cams=%s: connected, status=%s',
                             sorted(self.target_cams), r.status_code)
                    for chunk in r.iter_content(chunk_size=65536):
                        if self._stop.is_set():
                            break
                        if chunk:
                            self.parser.feed(chunk)
            except Exception as e:
                log.warning('GroupPuller cams=%s: %s -- reconnect in 5s',
                            sorted(self.target_cams), e)
                time.sleep(5)

    def stop(self):
        self._stop.set()


# Default cam grouping: 4+4+3 = 3 parallel DVR pulls. Empirically (pre-rewrite)
# yielded 10-14 fps per cam, balanced across all 11 cameras.
DEFAULT_CAM_GROUPS = [
    [0, 1, 2, 3],
    [4, 5, 6, 7],
    [8, 9, 10],
]


def parse_cam_groups(spec):
    groups = []
    for part in spec.split(':'):
        groups.append([int(x) for x in part.split(',') if x.strip()])
    return groups


class DvrDemux:
    def __init__(self, dvr_url, cam_groups=None, num_cams=NUM_CAMERAS,
                 target_fps=25.0):
        base_url = _strip_query(dvr_url)
        self.base_url = base_url
        self.cameras = {
            i: CameraStream(i, target_fps=target_fps) for i in range(num_cams)
        }
        groups = cam_groups or DEFAULT_CAM_GROUPS
        self.pullers = [
            DvrGroupPuller(base_url, group, self.cameras) for group in groups
        ]
        self._threads = []
        self.start_time = time.time()
        # Observability-only: periodic per-cam inter-arrival histogram dump.
        self._hist_logger_stop = threading.Event()
        self._hist_logger_thread = threading.Thread(
            target=self._hist_logger_loop,
            name='hist-logger',
            daemon=True,
        )
        self._hist_logger_thread.start()

    def _hist_logger_loop(self, interval_s=30.0):
        # First dump after `interval_s` so the buckets are populated.
        while not self._hist_logger_stop.wait(interval_s):
            for cam_idx in range(NUM_CAMERAS):
                cs = self.cameras.get(cam_idx)
                if cs is None:
                    continue
                with cs.lock:
                    hist = dict(cs._inter_arrival_hist)
                    total_frames = cs.total_frames
                if not hist:
                    log.info('cam%d inter-arrival: no frames yet', cam_idx)
                    continue
                parts = []
                for _, label in _INTER_ARRIVAL_BUCKETS:
                    n = hist.get(label, 0)
                    if n:
                        parts.append(f'{label}={n}')
                tail = hist.get('>10s', 0)
                if tail:
                    parts.append(f'>10s={tail}')
                log.info('cam%d inter-arrival (cum, frames=%d): %s',
                         cam_idx, total_frames, ' '.join(parts) or '(empty)')

    def run(self):
        log.info('DvrDemux starting, %d group pullers, base=%s',
                 len(self.pullers), self.base_url)
        for p in self.pullers:
            t = threading.Thread(
                target=p.run,
                name=f"Puller-{'.'.join(map(str, sorted(p.target_cams)))}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)
        for t in self._threads:
            t.join()

    def stop(self):
        self._hist_logger_stop.set()
        for p in self.pullers:
            p.stop()
        for cs in self.cameras.values():
            cs.stop()

    def stats(self):
        runtime = time.time() - self.start_time
        per_cam = {}
        total_chunks = 0
        total_sub = 0
        sub_types = defaultdict(int)
        dropped = 0
        per_channel_sub_types = defaultdict(lambda: defaultdict(int))
        for puller in self.pullers:
            total_chunks += puller.total_chunks
            total_sub += puller.total_sub_chunks
            dropped += puller.dropped_off_target
            for k, v in puller.sub_type_counts.items():
                sub_types[k] += v
            for ch, type_counts in puller.per_channel_sub_type_counts.items():
                for t, n in type_counts.items():
                    per_channel_sub_types[ch][t] += n
        for i, cs in self.cameras.items():
            with cs.lock:
                per_cam[i] = dict(
                    frames=cs.total_frames,
                    bytes=cs.total_bytes,
                    fps=cs.total_frames / max(runtime, 1),
                    consumers=len(cs.consumers),
                    last_age_s=(time.time() - cs.last_frame_ts) if cs.last_frame_ts else None,
                    input_dropped=cs.input_dropped,
                    input_qsize=cs._input_ring.qsize(),
                    consumer_dropped=sum(r.dropped for r in cs.consumers),
                    inter_arrival_hist=dict(cs._inter_arrival_hist),
                    sub_type_counts=dict(per_channel_sub_types.get(i, {})),
                )
        return dict(
            runtime_s=runtime,
            total_chunks=total_chunks,
            total_sub_chunks=total_sub,
            sub_type_counts=dict(sub_types),
            dropped_off_target_sub_chunks=dropped,
            num_groups=len(self.pullers),
            cameras=per_cam,
        )


class StreamHandler(http.server.BaseHTTPRequestHandler):
    demux = None

    def log_message(self, fmt, *args):
        log.debug('%s - %s', self.address_string(), fmt % args)

    def do_GET(self):
        url = urlparse(self.path)
        path = url.path
        if path == '/stats':
            return self._send_stats()
        if path == '/healthz':
            return self._send_health()
        if path.startswith('/cam'):
            try:
                cam_idx = int(path[4:])
            except ValueError:
                return self.send_error(404)
            if cam_idx not in self.demux.cameras:
                return self.send_error(404)
            return self._send_cam(cam_idx)
        self.send_error(404)

    def _send_health(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'ok\n')

    def _send_stats(self):
        body = json.dumps(self.demux.stats(), indent=2).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_cam(self, cam_idx):
        cs = self.demux.cameras[cam_idx]
        # Match the original VideoDispatchThread queue cap: 441 frames.
        # At 25 fps that's ~17 s of buffer; on overflow we drop everything
        # before the latest buffered I-frame (see FrameRingBuffer).
        ring = FrameRingBuffer(maxsize=441)
        cs.add_consumer(ring)
        try:
            self.send_response(200)
            self.send_header('Content-Type', 'video/h264')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            log.info('Consumer connected: cam%d', cam_idx)
            while True:
                try:
                    frame, _is_keyframe = ring.get(timeout=10)
                except queue.Empty:
                    log.warning('cam%d: no frame in 10s', cam_idx)
                    break
                try:
                    self.wfile.write(frame)
                except (BrokenPipeError, ConnectionResetError):
                    break
        finally:
            ring.stop()
            cs.remove_consumer(ring)
            log.info('Consumer disconnected: cam%d (dropped=%d)',
                     cam_idx, ring.dropped)


class ThreadedTCPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dvr-url', required=True)
    p.add_argument('--listen', default='0.0.0.0')
    p.add_argument('--port', type=int, default=8557)
    p.add_argument('--log-level', default='INFO')
    p.add_argument('--cam-groups', default=None,
                   help='Cam grouping spec, e.g. "0,1,2,3:4,5,6,7:8,9,10". '
                        'Default: 4+4+3.')
    p.add_argument('--target-fps', type=float, default=25.0,
                   help='Per-camera output pacing rate (default 25.0 fps). '
                        'Smooths source-side bursts. Set to 0 to disable '
                        'pacing entirely (raw burst forwarding).')
    args = p.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format='%(asctime)s %(levelname)s %(name)s | %(message)s',
    )

    cam_groups = parse_cam_groups(args.cam_groups) if args.cam_groups else None
    demux = DvrDemux(args.dvr_url, cam_groups=cam_groups,
                     target_fps=args.target_fps)
    StreamHandler.demux = demux

    t = threading.Thread(target=demux.run, name='DvrPuller', daemon=True)
    t.start()

    server = ThreadedTCPServer((args.listen, args.port), StreamHandler)
    log.info('HTTP server on %s:%d', args.listen, args.port)
    log.info('Per-cam streams: http://<host>:%d/cam<0..%d>', args.port, NUM_CAMERAS - 1)
    log.info('Stats endpoint: http://<host>:%d/stats', args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info('shutdown')
        demux.stop()


if __name__ == '__main__':
    main()
