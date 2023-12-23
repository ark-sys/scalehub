# Base image
FROM python:slim-bullseye

# Set default env variables
ENV TZ="Europe/Paris"
ENV KUBECONFIG="/app/conf/kubeconfig"
ENV ANSIBLE_CONFIG="/app/conf/ansible.cfg"
ENV PATH="$PATH:/app/script"

# Get build arguments, default to 1000:1000 if not set
ARG UID=1000
ARG GID=1000
# Eric B. is on the cut and my name is
ARG USER=rakim

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    iproute2 \
    procps \
    openssh-client \
    python3-pip \
    gcc \
    libffi-dev \
    curl \
    git \
    rustc \
    fish \
    iputils-ping \
    tk \
    nginx \
    sudo

# Install python requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Install kubectl
RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin

# Install Helm
RUN cd /tmp && curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && chmod 700 get_helm.sh && ./get_helm.sh && rm ./get_helm.sh

# Install auto-complete for kubectl
COPY fish/kubectl.fish /usr/share/fish/vendor_completions.d/
# RUN curl -o /usr/share/fish/vendor_completions.d/kubectl.fish https://raw.githubusercontent.com/evanlucas/fish-kubectl-completions/main/completions/kubectl.fish

# Install auto-completion for shub
COPY fish/shub.fish /usr/share/fish/vendor_completions.d/shub.fish

# Set host user id and group id without password
RUN groupadd -g $GID $USER && \
    useradd -m -u $UID -g $GID -o -s /usr/bin/fish $USER && \
    echo "$USER ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
# Set username
USER $USER

# Copy the lines to modify .bashrc and jump directly to fish at container spawn
RUN echo 'if [[ $(ps --no-header --pid=$PPID --format=comm) != "fish" && -z ${BASH_EXECUTION_STRING} ]]; then\n\
    shopt -q login_shell && LOGIN_OPTION="--login" || LOGIN_OPTION=""\n\
    exec fish $LOGIN_OPTION\n\
fi' >> ~/.bashrc

COPY entrypoint.sh /entrypoint.sh

RUN sudo chmod +x /entrypoint.sh

# Copy reverse-proxy config
COPY nginx/nginx.conf /etc/nginx/conf.d/scalehub.conf

# Create working directory for the script at the root. This easily allows to mount a volume to this directory
# Also set the permissions to rwx for all users to avoid permission issues
RUN sudo mkdir -m a=rwx /app/

# Set the working directory to /app
WORKDIR /app/

ENTRYPOINT ["/entrypoint.sh"]