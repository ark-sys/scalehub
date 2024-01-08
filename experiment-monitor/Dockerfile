FROM python:3.12-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir -p /app
RUN mkdir -p /app/conf

# Copy source code
COPY experiment-monitor.py /app/experiment-monitor.py
COPY utils /app/utils
COPY templates /app/templates

# Set working directory
WORKDIR /app

# Expose port 1883 for MQTT
EXPOSE 1883

ENV MQTT_BROKER_HOST="localhost"
ENV MQTT_BROKER_PORT="1883"
ENV MQTT_BROKER_USERNAME="monitor"
ENV MQTT_BROKER_PASSWORD="m_password"
ENV KUBECONFIG="/app/conf/kubeconfig"
# Redirect python script output to container logs
ENV PYTHONUNBUFFERED=1

## Run the application
ENTRYPOINT ["python3", "/app/experiment-monitor.py"]

## Hang the entrypoint so the container doesn't exit
#CMD ["tail", "-f", "/dev/null"]