FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install extra modules
RUN pip install --no-cache-dir gunicorn eventlet gevent

# Copy app source
COPY web/ /app

EXPOSE 5000
EXPOSE 5001

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "app:app"]
