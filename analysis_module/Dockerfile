FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY analysis_module /app/analysis_module

CMD ["python", "-u", "-m", "analysis_module.duckdb_analysis"]
