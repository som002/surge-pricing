FROM python:3.9-slim

WORKDIR /app

# Install java for PySpark
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Environment variables can be passed at runtime
# CMD can be overridden to run data_generator.py or surge.py
CMD ["python", "surge.py"]
