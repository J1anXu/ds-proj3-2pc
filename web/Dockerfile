FROM python:3.11-slim

WORKDIR /app

# Install system packages for gcc
RUN apt-get update && apt-get install -y \
    build-essential && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire web app
COPY . /app

CMD ["python", "app.py"]
