# icatch-dvr-demux

A small Python service that talks to the HTTP CGI interface of CCTV DVRs based
on the **iCatch SoC** family (sold under brand names like Vantech iWatch, and
others that ship the `iWatchDVR.exe` / `MoaiRobot.dylib` / `MayaRobot.dll`
client), parses the proprietary multi-camera frame format, and re-emits each
camera as a separate raw H.264 stream over HTTP — ready for `ffmpeg`,
[go2rtc](https://github.com/AlexxIT/go2rtc), [Frigate](https://frigate.video/),
or anything else that can consume an Annex-B byte stream.

```
   ┌──────────────┐  multipart/x-mixed-replace    ┌─────────────────────────┐
   │  iCatch DVR  │ ────────────────────────────► │      this service       │
   │              │  /cgi-bin/net_video.cgi       │  (Python, HTTP, :8557)  │
   └──────────────┘                               └────┬────────────────────┘
                                                       │  http://host:8557/camN
                                                       │  raw H.264 Annex-B
                                                       ▼
                                                  ffmpeg / go2rtc / Frigate
```

## Why this exists

Stock DVR firmware exposes a single multipart stream that interleaves all
cameras and is rejected by every off-the-shelf decoder. This service parses
the chunk format documented below and demultiplexes it so downstream tools
can treat each camera as a normal H.264 source.

## What you get on the HTTP side

| Endpoint           | Response                                                 |
|--------------------|----------------------------------------------------------|
| `GET /cam<N>`      | `video/h264` — live Annex-B byte stream for camera N     |
| `GET /stats`       | `application/json` — per-cam frame counters, fps, drops  |
| `GET /healthz`     | `text/plain` — `ok`                                      |

When a consumer connects to `/cam<N>`, the service first replays the most
recent cached SPS+PPS+IDR so the decoder can start immediately, then forwards
the live stream.

### About the output

What comes out of `/cam<N>` is **raw H.264 Annex-B with no container and no
timestamps**. The DVR never tells us when a frame was captured — there's just
a sequence of NAL units. Consumers must generate timestamps themselves
(wall-clock works fine for live monitoring); ffmpeg does this with
`-use_wallclock_as_timestamps 1`. If you wrap the stream into a container,
do that downstream.

## Two things worth knowing about the upstream DVR

### One camera per HTTP pull = full fps

The CGI endpoint accepts a bitmask telling the DVR which cameras to include
in a given multipart stream. If you ask for all cameras in one request the
DVR shares the encoder budget across them and you get a fraction of the
nominal fps per camera. **One HTTP request per camera gives you the highest
per-camera frame rate** — at the cost of one TCP/HTTP session per camera
upstream. Configure via `--cam-groups`; the example compose uses
`0:1:2:3:4:5:6:7:8:9:10` (eleven separate pulls).

### Frames arrive in bursts, not evenly spaced

The DVR encoder emits frames in clumps (a few frames close together, then
silence) rather than at a steady cadence. If your downstream consumer needs
even pacing (e.g. some recorders, some motion-detection pipelines), you have
two options:

1. **Pace inside this service** — set `--target-fps 25` (or whatever rate
   you want). The dispatcher will hold frames and release them at the target
   interval. Adds latency equal to the burst spread.
2. **Pace downstream** — leave `--target-fps 0` (default forwards as-is)
   and let your consumer smooth it. For ffmpeg/go2rtc:
   ```
   -vf fps=25,setpts=N/25/TB
   ```

Pick whichever fits where the rest of your encode pipeline already runs.

## Quickstart (Docker)

```bash
# .env in the project dir:
echo "DVR_URL=http://admin:YOUR_PASS@dvr.example.lan/cgi-bin/net_video.cgi" > .env

docker compose -f docker-compose.example.yml up -d

# verify
curl -s http://localhost:8557/stats | jq
ffmpeg -f h264 -i http://localhost:8557/cam0 -frames:v 1 cam0.png
```

## Quickstart (bare metal)

```bash
pip install requests
python dvr_demux_serve.py \
  --dvr-url 'http://admin:YOUR_PASS@dvr.example.lan/cgi-bin/net_video.cgi' \
  --listen 0.0.0.0 --port 8557 \
  --cam-groups '0:1:2:3:4:5:6:7:8:9:10' \
  --target-fps 0
```

The query string of `--dvr-url` is ignored — the service builds the correct
CGI parameter set internally.

## CLI flags

| Flag              | Default     | Notes                                                                  |
|-------------------|-------------|------------------------------------------------------------------------|
| `--dvr-url`       | (required)  | DVR CGI URL with credentials. Query string is stripped and rewritten.  |
| `--listen`        | `0.0.0.0`   | HTTP bind address.                                                     |
| `--port`          | `8557`      | HTTP listen port.                                                      |
| `--cam-groups`    | `0,1,2,3:4,5,6,7:8,9,10` | Colon-separated groups; each group = one upstream HTTP pull. |
| `--target-fps`    | `25.0`      | Per-cam pacing rate. `0` = no pacing, forward raw bursts.              |
| `--log-level`     | `INFO`      | `DEBUG`, `INFO`, `WARNING`, `ERROR`.                                   |

## Integrating with go2rtc / Frigate

Point go2rtc at one HTTP source per camera. Because the stream has no
timestamps, instruct ffmpeg to fabricate them from the wall clock and (if
you want even pacing) apply an fps filter:

```yaml
go2rtc:
  ffmpeg:
    # keep this on ONE line — go2rtc inlines it verbatim into the ffmpeg cmd,
    # and YAML folded scalars (`>`) leave a trailing newline that breaks it.
    raw_h264: "-loglevel warning -fflags +genpts+igndts -use_wallclock_as_timestamps 1 -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 2 -thread_queue_size 512 -rtbufsize 256M -f h264 -i {input}"
  streams:
    cam0: ffmpeg:http://dvr-demux-host:8557/cam0#input=raw_h264
    cam1: ffmpeg:http://dvr-demux-host:8557/cam1#input=raw_h264
    # ...
```

In Frigate, point each camera at the go2rtc restream
(`rtsp://127.0.0.1:8554/<name>`).

## Hardware this is known to work with

- Vantech iWatch DVR (rebranded iCatch SoC, 11 cameras, MayaRobot Mac/Win
  client)

If you have another DVR that ships an iWatchDVR-like client and uses the
same `/cgi-bin/net_video.cgi` CGI endpoint, this is likely to work. If it
doesn't, open an issue with a packet capture and we can extend the parser.

## Format reference (so the next person doesn't have to reverse it again)

The DVR replies with `Content-Type: multipart/x-mixed-replace;
boundary=myboundary`. Each multipart section has standard HTTP headers, a
blank line, and then a binary body. The body starts with magic `0x00001234`
followed by an opaque header and a stream of fixed-size sub-chunk headers,
each with a small payload.

### Top-level header (288 bytes)

| Offset | Type    | Field         | Notes                          |
|--------|---------|---------------|--------------------------------|
| `0x00` | u32 LE  | `magic`       | `0x00001234`                   |
| `0x0c` | u32 LE  | `ts1`         | Capture timestamp (encoder)    |
| `0x10` | u32 LE  | `ts2`         | Secondary timestamp            |
| `0x1c` | u32 LE  | `chunk_count` | Number of sub-chunks following |
| `0x120`| —       | —             | First sub-chunk header starts  |

### Sub-chunk header (44 bytes / 0x2c) + variable payload

| Offset | Type    | Field      | Notes                                                       |
|--------|---------|------------|-------------------------------------------------------------|
| `0x00` | u32     | `type`     | `0`=I-frame, `1`=P-frame, `2`=audio, `3/4`=reserved/skip    |
| `0x04` | u32     | `channel`  | Camera index (0..10)                                        |
| `0x08` | u32     | `width`    | e.g. 1920                                                   |
| `0x0c` | u32     | `height`   | e.g. 1080                                                   |
| `0x24` | u32     | `size`     | Payload size                                                |
| `0x28` | u32     | `next_step`| Offset from sub-chunk-body-start to next sub-chunk header   |
| `0x2c` | bytes   | payload    | Raw H.264 NALs (for type 0/1)                               |

### Iteration

```python
off = 0x120
for _ in range(chunk_count):
    sub_type, channel, size, next_step = parse_sub_header(buf, off)
    payload = buf[off + 0x2c : off + 0x2c + size]
    handle(sub_type, channel, payload)
    off = off + 0x2c + next_step
```

### CGI URL parameters

```
/cgi-bin/net_video.cgi?hq=1&iframe=BM&pframe=BM&audio=0&complete=0&beg=-1&end=-1&ivs=0
```

| Param      | Value (live)             | Meaning                                              |
|------------|--------------------------|------------------------------------------------------|
| `hq`       | `1`                      | High quality                                         |
| `iframe`   | bitmask `1<<cam`         | Cameras to deliver I-frames for                      |
| `pframe`   | bitmask `1<<cam`         | Cameras to deliver P-frames for                      |
| `audio`    | `0`                      | Audio sub-chunks (bitmask). Ignored downstream.      |
| `complete` | **`0`**                  | `1` triggers replay/burst mode — do not use for live |
| `beg`/`end`| `-1` / `-1`              | Replay time window (Unix epoch); `-1` = live         |
| `ivs`      | `0`                      | IVS metadata bitmask (motion, line crossing, …)      |

Source-of-truth for the field meanings is `RemoteStream::Start` and
`RemoteStream::Split` in the DVR vendor's `MoaiRobot.dylib` (the dynamically
loaded body of `iWatchDVR.exe`/`.app`). The vendor binaries themselves are
not redistributed in this repo for licensing reasons.

## License

MIT — see [LICENSE](LICENSE).
