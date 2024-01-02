FROM python:3.9.5-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir -p /app
RUN mkdir -p /app/conf

# Copy source code
COPY experiment-monitor.py /app/experiment-monitor.py
COPY utils /app/utils

# Set working directory
WORKDIR /app

# Expose port 1883 for MQTT
EXPOSE 1883

ENV MQTT_BROKER_HOST="localhost"
ENV MQTT_BROKER_PORT="1883"
ENV KUBECONFIG="/app/conf/kubeconfig"

## Run the application
ENTRYPOINT ["python3", "/app/experiment-monitor.py"]

## Hang the entrypoint so the container doesn't exit
#CMD ["tail", "-f", "/dev/null"]