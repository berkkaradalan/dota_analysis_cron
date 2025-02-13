FROM ubuntu:20.04

WORKDIR /app

COPY requirements.txt /app/

RUN apt-get update && \
    apt-get install -y python3 python3-venv python3-pip cron && \
    python3 -m venv /venv && \
    /venv/bin/pip install --upgrade pip && \
    /venv/bin/pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY crontab /etc/cron.d/custom_cron
COPY entrypoint.sh /entrypoint.sh

RUN echo "" >> /etc/cron.d/custom_cron && \
    chmod 0644 /etc/cron.d/custom_cron && \
    crontab /etc/cron.d/custom_cron

RUN chmod +x /entrypoint.sh main.py

CMD ["/entrypoint.sh"]
