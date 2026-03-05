FROM python:3.12-slim

LABEL maintainer="roel"
LABEL description="BfArM drug shortage monitor — tracks configurable medications via WATCH_LIST"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY monitor.py .

RUN groupadd -r app && useradd -r -g app app \
    && mkdir -p /data && chown app:app /data

VOLUME /data

USER app

CMD ["python", "-u", "monitor.py"]
