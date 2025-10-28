FROM python:3.12-slim

# Install ssh client for ansible
RUN apt-get update && apt-get install -y ssh && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN --mount=type=bind,source=setup/monitor/requirements.txt,target=requirements.txt pip install -r requirements.txt

RUN mkdir -p /app /app/conf /app/src /app/templates /app/playbooks /tmp/ansible

# Copy source code
ADD src/monitor/ /app/src/monitor/
ADD src/utils /app/src/utils
ADD src/src /app/src/src

# Add templates
ADD playbooks /app/playbooks
ADD playbooks/application/roles/load_generators/templates/* /app/templates/
ADD playbooks/application/roles/chaos/templates/* /app/templates/

# Create __init__.py file to make the directory a package
RUN touch /app/src/__init__.py

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
# Redirect python src output to container logs
ENV PYTHONUNBUFFERED=1

## Run the application
ENTRYPOINT ["python3", "/app/src/monitor/monitor.py"]

## DEBUG: Hang the entrypoint so the container doesn't exit
#CMD ["tail", "-f", "/dev/null"]