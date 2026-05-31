FROM python:3.11-slim

WORKDIR /app
COPY bonus_backfill_agent/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY bonus_backfill_agent/agent.py /app/agent.py
CMD ["python", "/app/agent.py"]
