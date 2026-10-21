FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir orjson==3.10.18
COPY seedwatch /app/seedwatch
COPY dashboard /app/dashboard
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "seedwatch.cli"]
