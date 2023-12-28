FROM python:3.9.5-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir -p /app

# Copy source code
COPY experiment-monitor.py /app/experiment-monitor.py
COPY utils /app/utils

# Set working directory
WORKDIR /app

# Expose port 1883 for MQTT
EXPOSE 1883

# Run the application
CMD ["python", "/experiment-monitor.py"]