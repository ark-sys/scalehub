# Base image
FROM python:3.10-bookworm

# Set default env variables
ENV TZ="Europe/Paris"
ENV KUBECONFIG="/app/conf/kubeconfig"
# Some ansible modules use the K8S_AUTH_KUBECONFIG variable instead of KUBECONFIG
ENV K8S_AUTH_KUBECONFIG="/app/conf/kubeconfig"
ENV ANSIBLE_CONFIG="/app/conf/ansible.cfg"
ENV SHUB_CONFIG="/app/conf/scalehub.ini"
ENV PATH="$PATH:/app/scripts"
ENV PYTHONPATH="/app:/app/scripts"
# Silence warnirngs related to dependencies
ENV PYTHONWARNINGS=ignore::UserWarning
# Get build arguments, default to 1000:1000 if not set
ARG UID=1000
ARG GID=1000
# Eric B. is on the cut and my name is
ARG USER=rakim

# Create working directory for the scripts at the root. This easily allows to mount a volume to this directory
# Also set the permissions to rwx for all users to avoid permission issues
RUN mkdir -m a=rwx /app/

# Set the working directory to /app
WORKDIR /app/

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    iproute2 \
    procps \
    openssh-client \
    gcc \
    libffi-dev \
    curl \
    git \
    rustc \
    fish \
    iputils-ping \
    tk \
    nginx \
    sudo \
    rsync \
    cron  \
    nano  \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install python requirements. Mounting the file helps with caching
RUN --mount=type=bind,source=requirements.txt,target=requirements.txt pip install -r requirements.txt

# Install kubectl
RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin

# Install Helm
RUN cd /tmp && curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && chmod 700 get_helm.sh && ./get_helm.sh && rm ./get_helm.sh

# Install auto-complete for kubectl
ADD fish/kubectl.fish /usr/share/fish/vendor_completions.d/kubectl.fish

# Install auto-completion for shub
ADD fish/shub.fish /usr/share/fish/vendor_completions.d/shub.fish

# Install some custom functions to enhance prompt
ADD fish/functions/* /usr/share/fish/functions/

# Copy reverse-proxy config
ADD nginx/nginx.conf /etc/nginx/conf.d/scalehub.conf

# Set host user id and group id without password
RUN groupadd -g $GID $USER && \
    useradd -m -u $UID -g $GID -o -s /usr/bin/fish $USER && \
    echo "$USER ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Modify .bashrc to jump directly into fish at container spawn
RUN echo 'if [[ $(ps --no-header --pid=$PPID --format=comm) != "fish" && -z ${BASH_EXECUTION_STRING} ]]; then\n\
    shopt -q login_shell && LOGIN_OPTION="--login" || LOGIN_OPTION=""\n\
    exec fish $LOGIN_OPTION -d3\n\
fi' >> ~/.bashrc

# Setup the entrypoint
ADD --chmod=755 entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# Download and install the mitogen plugin for Ansible
RUN mkdir -p /app/ansible

ADD https://files.pythonhosted.org/packages/source/m/mitogen/mitogen-0.3.7.tar.gz /app/ansible/mitogen-plugin.tar.gz

RUN tar -xzf /app/ansible/mitogen-plugin.tar.gz -C /app/ansible && \
    rm /app/ansible/mitogen-plugin.tar.gz && \
    mv /app/ansible/mitogen-0.3.7 /app/ansible/mitogen

# Image is ready, switch to the user
USER $USER

# Install additional Ansible collections
RUN ansible-galaxy collection install bitwarden.secrets

