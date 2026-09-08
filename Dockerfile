FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gateway/ gateway/
COPY mtproto/ mtproto/

RUN useradd -r -u 10001 gateway && mkdir -p /data && chown gateway /data
USER gateway
VOLUME /data
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=3).status==200 else 1)"

# Bot API gateway by default; for Pyrogram apps use the mtproto layer:
#   docker run tg-session-gateway python -m mtproto.entrypoint your.app
CMD ["python", "-m", "gateway.main"]
