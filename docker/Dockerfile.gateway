FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gateway/ gateway/

RUN useradd -r -u 10001 gateway && mkdir -p /data && chown gateway /data
USER gateway
VOLUME /data
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=3).status==200 else 1)"

CMD ["python", "-m", "gateway.main"]
