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
# Parametrize MITOGEN_VERSION
ARG MITOGEN_VERSION=0.3.14
# Parametrize K9S_VERSION
ARG K9S_VERSION=v0.32.5


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
    vim \
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

# Copy history file from /app/conf to initialize some command history autocomplete
ADD --chown=$UID:$GID fish/fish_history /home/$USER/.local/share/fish/

# Install K9s
RUN wget https://github.com/derailed/k9s/releases/download/$K9S_VERSION/k9s_linux_amd64.deb && apt install ./k9s_linux_amd64.deb && rm k9s_linux_amd64.deb

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

ADD https://files.pythonhosted.org/packages/source/m/mitogen/mitogen-$MITOGEN_VERSION.tar.gz /app/ansible/mitogen-plugin.tar.gz

RUN tar -xzf /app/ansible/mitogen-plugin.tar.gz -C /app/ansible && \
    rm /app/ansible/mitogen-plugin.tar.gz && \
    mv /app/ansible/mitogen-$MITOGEN_VERSION /app/ansible/mitogen
 \
#============================================================================================
# Image is ready, switch to the user
USER $USER

# Create private_dir for Ansible
RUN mkdir -p /tmp/ansible

# Create runtime folder for scalehub
RUN mkdir -p /app/conf/runtime

# Install additional Ansible collections
RUN ansible-galaxy collection install bitwarden.secrets kubernetes.core

# Install krew plugin for kubectl
RUN (    set -x; cd "$(mktemp -d)" && \
              OS="$(uname | tr '[:upper:]' '[:lower:]')" && \
              ARCH="$(uname -m | sed -e 's/x86_64/amd64/' -e 's/\(arm\)\(64\)\?.*/\1\2/' -e 's/aarch64$/arm64/')" && \
              KREW="krew-${OS}_${ARCH}" && \
              curl -fsSLO "https://github.com/kubernetes-sigs/krew/releases/latest/download/${KREW}.tar.gz" && \
              tar zxvf "${KREW}.tar.gz" && \
              ./"${KREW}" install krew )
ENV PATH="${PATH}:/home/$USER/.krew/bin"

# Install netshoot plugin for kubectl
RUN kubectl krew index add netshoot https://github.com/nilic/kubectl-netshoot.git
RUN kubectl krew install netshoot/netshoot