FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install Python dependencies from the repo root requirements
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy only analysis code to keep the image smaller
COPY analysis /app/analysis

WORKDIR /app/analysis
CMD ["python", "-u", "duckdb_analysis.py"]
