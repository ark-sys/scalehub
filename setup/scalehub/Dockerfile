# Base image
FROM python:3.10-bookworm
LABEL maintainer="Khaled Arsalane"

# Set build arguments early
ARG UID=1000
ARG GID=1000
# Eric B. is on the cut and my name is
ARG USER=rakim
ARG MITOGEN_VERSION=0.3.22
ARG K9S_VERSION=v0.50.9
ARG KUBECTL_VERSION=v1.31.0
ARG KREW_VERSION=v0.4.5
ARG KREW=krew-linux_amd64

# Set environment variables that don't change
ENV TZ="Europe/Paris" \
    KUBECONFIG="/app/runtime/kubeconfig" \
    ANSIBLE_CONFIG="/app/conf/ansible.cfg" \
    PYTHONWARNINGS=ignore::UserWarning \
    KREW_ROOT="/app/.krew"

ENV K8S_AUTH_KUBECONFIG=$KUBECONFIG \
    PATH="$PATH:/app/scripts:/app/.krew/bin" \
    PYTHONPATH="/app:/app/scripts"

# Install system packages in one layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    iproute2 openssh-client openssh-server gcc libffi-dev curl git \
    rustc fish iputils-ping tk sudo rsync cron nano vim nginx \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create user and directories early
RUN groupadd -g $GID $USER && \
    useradd -m -u $UID -g $GID -o -s /usr/bin/bash $USER && \
    echo "$USER ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    mkdir -m a=rwx /app/

# Install Python requirements (cache-friendly)
RUN --mount=type=bind,source=setup/scalehub/requirements.txt,target=requirements.txt pip install -r requirements.txt

# Download and install Kubernetes tools in parallel stages
ADD --chmod=755 https://storage.googleapis.com/kubernetes-release/release/$KUBECTL_VERSION/bin/linux/amd64/kubectl /usr/local/bin/kubectl

ADD --chmod=700 https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 /tmp/get_helm.sh
RUN /tmp/get_helm.sh && rm /tmp/get_helm.sh

ADD --chmod=755  https://github.com/derailed/k9s/releases/download/$K9S_VERSION/k9s_linux_amd64.deb /tmp/k9s_linux_amd64.deb
RUN apt install /tmp/k9s_linux_amd64.deb && rm /tmp/k9s_linux_amd64.deb

## Install krew plugin for kubectl
ADD https://github.com/kubernetes-sigs/krew/releases/download/${KREW_VERSION}/${KREW}.tar.gz /app/.krew/krew.tar.gz
RUN tar zxvf /app/.krew/krew.tar.gz -C /app/.krew && \
    rm /app/.krew/krew.tar.gz && \
    /app/.krew/${KREW} install krew

WORKDIR /app/

# Install kubectl plugins as user
RUN kubectl krew index add netshoot https://github.com/nilic/kubectl-netshoot.git && \
    kubectl krew install netshoot/netshoot

# Install Ansible collections
RUN ansible-galaxy collection install kubernetes.core --collections-path /home/$USER/.ansible/collections

# Download and install mitogen
ADD --chmod=644 https://files.pythonhosted.org/packages/source/m/mitogen/mitogen-$MITOGEN_VERSION.tar.gz /app/ansible/mitogen-plugin.tar.gz

RUN tar -xzf /app/ansible/mitogen-plugin.tar.gz -C /app/ansible && \
    rm /app/ansible/mitogen-plugin.tar.gz && \
    mv /app/ansible/mitogen-$MITOGEN_VERSION /app/ansible/mitogen

# Copy configuration files
ADD setup/scalehub/fish/functions/* /usr/share/fish/functions/
ADD setup/scalehub/fish/shub.fish /usr/share/fish/vendor_completions.d/shub.fish
ADD setup/scalehub/fish/netshoot.fish /usr/share/fish/vendor_completions.d/kubectl_netshoot.fish

ADD setup/scalehub/secrets/scalehub.pub /home/$USER/.ssh/authorized_keys

# ADD nginx configuration
ADD setup/scalehub/nginx/nginx.conf /etc/nginx/nginx.conf
ADD setup/scalehub/nginx/conf.d/http /etc/nginx/conf.d/http
ADD setup/scalehub/nginx/conf.d/upstreams /etc/nginx/conf.d/upstreams
ADD setup/scalehub/nginx/home/ /usr/share/nginx/html/
# Generate ssl certificates
ADD setup/scalehub/nginx/conf.d/ssl/openssl.cnf /etc/nginx/openssl.cnf

RUN openssl req -new -newkey rsa:2048 -days 10950 -nodes -x509 -keyout /etc/ssl/private/nginx-selfsigned.key -out /etc/ssl/certs/nginx-selfsigned.crt -subj "/C=FR/ST=Breizh/L=Roazhon/O=Magellan/CN=scalehub.dev" --reqexts SAN --extensions SAN -config /etc/nginx/openssl.cnf

# Setup shell configurations
RUN mkdir -p /home/$USER/.config/fish && \
    echo "kubectl completion fish | source" > /home/$USER/.config/fish/config.fish

# Copy application code
ADD scripts /app/scripts
ADD conf /app/conf
ADD playbooks /app/playbooks
ADD setup/scalehub/entrypoint.sh /entrypoint.sh

# Final setup
RUN mkdir -p /app/runtime /app/experiments-data && \
    chown -R $USER:$USER /app /home/$USER && \
    chmod +x /entrypoint.sh

USER $USER
SHELL ["/usr/bin/fish", "-c"]

RUN mkdir -p /tmp/ansible

EXPOSE 22 80 443
HEALTHCHECK --interval=5s --timeout=3s --retries=3 CMD curl -f http://localhost/ || exit 1
ENTRYPOINT ["/entrypoint.sh"]