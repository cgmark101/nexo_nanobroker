FROM python:3.14-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.14-slim
WORKDIR /app
COPY --from=builder /root/.local /root/.local
COPY .env.example .env
COPY main.py .

ENV PATH=/root/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    NANOBROKER_DB_FILE=/data/broker_local.db

VOLUME /data
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; exit(0 if urllib.request.urlopen('http://localhost:8000/health').read() else 1)"

CMD ["python", "main.py"]
