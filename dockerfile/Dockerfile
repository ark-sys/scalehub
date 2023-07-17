# Base image
FROM python:3.12-rc-slim

# Set the working directory
#WORKDIR /app
RUN mkdir /app/

# Set default env variables
ENV PATH="$PATH:/app/script"
ENV KUBECONFIG="/app/conf/kubeconfig"
ENV TZ="Europe/Paris"

# Replicate host user and group
ARG UID
ARG GID
ARG UNAME

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    openssh-client \
    python3-pip \
    gcc \
    libffi-dev \
    curl \
    git \
    rustc \
    fish \
    sudo

# Install kubectl
RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin

# Install Helm
RUN cd /tmp && curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && chmod 700 get_helm.sh && ./get_helm.sh && rm ./get_helm.sh

# Install python requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Setup host user within container with correct permissions
RUN groupadd -g $GID $UNAME &&  \
    useradd -m -d /home/$UNAME -u $UID -g $GID -s /bin/fish $UNAME &&  \
    chown -R $UNAME /home/$UNAME &&  \
    adduser $UNAME sudo && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER $UID:$GID
WORKDIR /home/$UNAME

# TODO add autocompletion for python script as describe here -> https://fishshell.com/docs/current/completions.html
ENTRYPOINT ["sleep", "infinity"]
