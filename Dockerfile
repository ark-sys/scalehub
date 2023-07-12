# Base image
FROM python:3.12-rc-slim

# Set the working directory
WORKDIR /app
RUN mkdir /app/script
ENV PATH="$PATH:/app/script"
ENV KUBECONFIG="/app/conf/kubeconfig"
ENV TZ="Europe/Paris"

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    openssh-client \
    python3-pip \
    gcc \
    libffi-dev \
    fish \
    curl \
    git \
    rustc

# Install kubectl
RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin

# Install Helm
RUN cd /tmp && curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && chmod 700 get_helm.sh && ./get_helm.sh && rm ./get_helm.sh

# Install python requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# TODO add autocompletion for python script as describe here -> https://fishshell.com/docs/current/completions.html
ENTRYPOINT ["sleep", "infinity"]
