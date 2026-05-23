FROM python:3.12-slim

RUN pip install --no-cache-dir --root-user-action=ignore requests

COPY dvr_demux_serve.py /app/dvr_demux_serve.py

EXPOSE 8557

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "/app/dvr_demux_serve.py"]
