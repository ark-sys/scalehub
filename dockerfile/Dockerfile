# Base image
FROM python:slim-bullseye

# Set the working directory
RUN mkdir /app/

# Set default env variables
ENV TZ="Europe/Paris"
ENV KUBECONFIG="/app/conf/kubeconfig"
ENV ANSIBLE_CONFIG="/app/conf/ansible.cfg"
ENV PATH="$PATH:/app/script"

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
    tk

# Install kubectl
RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin

# Install Helm
RUN cd /tmp && curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && chmod 700 get_helm.sh && ./get_helm.sh && rm ./get_helm.sh

# Install python requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Install auto-complete for kubectl
COPY kubectl.fish /usr/share/fish/vendor_completions.d/
# RUN curl -o /usr/share/fish/vendor_completions.d/kubectl.fish https://raw.githubusercontent.com/evanlucas/fish-kubectl-completions/main/completions/kubectl.fish

# Install auto-completion for shub
COPY shub.fish /usr/share/fish/vendor_completions.d/shub.fish

# Copy the lines to modify .bashrc and jump directly to fish at container spawn
RUN echo 'if [[ $(ps --no-header --pid=$PPID --format=comm) != "fish" && -z ${BASH_EXECUTION_STRING} ]]; then\n\
    shopt -q login_shell && LOGIN_OPTION="--login" || LOGIN_OPTION=""\n\
    exec fish $LOGIN_OPTION\n\
fi' >> ~/.bashrc

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]