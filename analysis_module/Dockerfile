FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY analysis /app/analysis

CMD ["python", "-u", "-m", "analysis.duckdb_analysis"]
