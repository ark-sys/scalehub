FROM python:3.12-slim

# Install dependencies
RUN --mount=type=bind,source=requirements.txt,target=requirements.txt pip install -r requirements.txt

RUN mkdir -p /app /app/conf /app/scripts

# Copy source code
ADD scripts/monitor/experiment-monitor.py /app/scripts/experiment-monitor.py
ADD scripts/monitor/templates /app/templates
ADD scripts/utils /app/scripts/utils

# Create __init__.py file to make the directory a package
RUN touch /app/scripts/__init__.py

# Set working directory
WORKDIR /app

# Expose port 1883 for MQTT
EXPOSE 1883

ENV MQTT_BROKER_HOST="localhost"
ENV MQTT_BROKER_PORT="1883"
ENV MQTT_BROKER_USERNAME="monitor"
ENV MQTT_BROKER_PASSWORD="m_password"
ENV KUBECONFIG="/app/conf/kubeconfig"
ENV PYTHONPATH="/app"
# Redirect python scripts output to container logs
ENV PYTHONUNBUFFERED=1

## Run the application
ENTRYPOINT ["python3", "/app/scripts/experiment-monitor.py"]

## DEBUG: Hang the entrypoint so the container doesn't exit
#CMD ["tail", "-f", "/dev/null"]