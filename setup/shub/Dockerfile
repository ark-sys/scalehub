# Base image
FROM python:3.10-bookworm
LABEL maintainer="Khaled Arsalane"

# Set default env variables
ENV TZ="Europe/Paris"
ENV KUBECONFIG="/app/runtime/kubeconfig"
# Some ansible modules use the K8S_AUTH_KUBECONFIG variable instead of KUBECONFIG
ENV K8S_AUTH_KUBECONFIG=$KUBECONFIG
ENV ANSIBLE_CONFIG="/app/conf/ansible.cfg"
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
ARG MITOGEN_VERSION=0.3.22
# Parametrize K9S_VERSION
ARG K9S_VERSION=v0.32.7
ARG KUBECTL_VERSION=v1.31.0

ARG KREW_VERSION=v0.4.4
ARG KREW=krew-linux_amd64

# Create working directory for the scripts at the root. This easily allows to mount a volume to this directory
# Also set the permissions to rwx for all users to avoid permission issues
RUN mkdir -m a=rwx /app/

# Set the working directory to /app
WORKDIR /app/

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    iproute2 \
    openssh-client \
    gcc \
    libffi-dev \
    curl \
    git \
    rustc \
    fish \
    iputils-ping \
    tk \
    sudo \
    rsync \
    cron  \
    nano  \
    vim \
    nginx \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


####### Base python modules #######

# Install python requirements. Mounting the file helps with caching
RUN --mount=type=bind,source=setup/shub/requirements.txt,target=requirements.txt pip install -r requirements.txt

####### Kubernetes stuff #######
# Install kubectl
ADD --chmod=755 https://storage.googleapis.com/kubernetes-release/release/$KUBECTL_VERSION/bin/linux/amd64/kubectl /usr/local/bin/kubectl

# Install Helm
ADD --chmod=700 https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 /tmp/get_helm.sh
RUN /tmp/get_helm.sh && rm /tmp/get_helm.sh

# Install K9s
ADD --chmod=755  https://github.com/derailed/k9s/releases/download/$K9S_VERSION/k9s_linux_amd64.deb /tmp/k9s_linux_amd64.deb
RUN apt install /tmp/k9s_linux_amd64.deb && rm /tmp/k9s_linux_amd64.deb

# Install auto-complete for kubectl
ADD setup/shub/fish/kubectl.fish /usr/share/fish/vendor_completions.d/kubectl.fish

ENV KREW_ROOT="/app/.krew"

## Install krew plugin for kubectl
ADD https://github.com/kubernetes-sigs/krew/releases/download/${KREW_VERSION}/${KREW}.tar.gz /app/.krew/krew.tar.gz
RUN tar zxvf /app/.krew/krew.tar.gz -C /app/.krew && \
    rm /app/.krew/krew.tar.gz && \
    /app/.krew/${KREW} install krew

ENV PATH="${PATH}:/app/.krew/bin"

# Install netshoot plugin for kubectl
RUN kubectl krew index add netshoot https://github.com/nilic/kubectl-netshoot.git
RUN kubectl krew install netshoot/netshoot

# Install auto-completion for kubectl netshoot
ADD setup/shub/fish/netshoot.fish /usr/share/fish/vendor_completions.d/kubectl_netshoot.fish

######## Scalehub environment setup ########
# Modify .bashrc to jump directly into fish at container spawn
RUN echo 'if [[ $(ps --no-header --pid=$PPID --format=comm) != "fish" && -z ${BASH_EXECUTION_STRING} ]]; then\n\
    shopt -q login_shell && LOGIN_OPTION="--login" || LOGIN_OPTION=""\n\
    exec fish $LOGIN_OPTION -d3\n\
fi' >> ~/.bashrc

# Install auto-completion for shub
ADD setup/shub/fish/shub.fish /usr/share/fish/vendor_completions.d/shub.fish

# Install some custom functions to enhance prompt
ADD setup/shub/fish/functions/* /usr/share/fish/functions/

# Copy history file from /app/conf to initialize some command history autocomplete
ADD setup/shub/fish/fish_history /root/.local/share/fish/

####### Nginx stuff #######
EXPOSE 80 443
# ADD nginx configuration
ADD setup/shub/nginx/nginx.conf /etc/nginx/nginx.conf
ADD setup/shub/nginx/conf.d/http /etc/nginx/conf.d/http
ADD setup/shub/nginx/conf.d/upstreams /etc/nginx/conf.d/upstreams
ADD setup/shub/nginx/home/* /usr/share/nginx/html/*


# Generate ssl certificates
ADD setup/shub/nginx/conf.d/ssl/openssl.cnf /etc/nginx/openssl.cnf
RUN openssl req -new -newkey rsa:2048 -days 10950 -nodes -x509 -keyout /etc/ssl/private/nginx-selfsigned.key -out /etc/ssl/certs/nginx-selfsigned.crt -subj "/C=FR/ST=Breizh/L=Roazhon/O=Magellan/CN=scalehub.dev" --reqexts SAN --extensions SAN -config /etc/nginx/openssl.cnf

# Add healtcheck on nginx
HEALTHCHECK --interval=5s --timeout=3s --retries=3 CMD curl -f http://localhost/ || exit 1

############ Switch to user to not break ssh ############
RUN groupadd -g $GID $USER && \
    useradd -m -u $UID -g $GID -o -s /usr/bin/fish $USER && \
    echo "$USER ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER $USER

# Setup the entrypoint
ADD --chmod=755 setup/shub/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
# ADD scripts data
ADD scripts /app/scripts
ADD conf /app/conf
ADD playbooks /app/playbooks

# Create runtime directory
RUN mkdir -p /app/runtime

####### Ansible stuff #######

# Download and install the mitogen plugin for Ansible
RUN mkdir -p /app/ansible /tmp/ansible
ADD --chmod=644 https://files.pythonhosted.org/packages/source/m/mitogen/mitogen-$MITOGEN_VERSION.tar.gz /app/ansible/mitogen-plugin.tar.gz

RUN tar -xzf /app/ansible/mitogen-plugin.tar.gz -C /app/ansible && \
    rm /app/ansible/mitogen-plugin.tar.gz && \
    mv /app/ansible/mitogen-$MITOGEN_VERSION /app/ansible/mitogen

# Install additional Ansible collections
RUN ansible-galaxy collection install bitwarden.secrets kubernetes.core


