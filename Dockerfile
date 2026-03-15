FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY data_landing/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy all pipeline code
COPY data_landing/ /app/data_landing/
COPY datamart/     /app/datamart/

# Copy orchestrator
COPY run_pipeline.py /app/run_pipeline.py

# Create directories
RUN mkdir -p /app/data_input /app/data_archive

CMD ["python", "run_pipeline.py"]
