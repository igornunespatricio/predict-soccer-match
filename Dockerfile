# Use Python 3.10 (required by azureml packages)
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies (some Azure packages need these)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip first
RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]