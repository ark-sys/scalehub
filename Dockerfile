# Base image
FROM python:3.12-rc-slim
ARG USER_ID
# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    openssh-client \
    python3-pip \
    gcc \
    libffi-dev \
    fish

# Install Ansible module for Python
RUN pip3 install wheel ansible enoslib

# Install kubectl
RUN apt-get update && apt-get install -y curl && \
    curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin

# Set the working directory
WORKDIR /app
RUN mkdir /app/script

#################################################
#TODO temporary
# use ARG CACHEBUST=1 for custom cache invalidation
RUN apt install iputils-ping iproute2 -y
## Adding support to ssh server for pycharm
#RUN mkdir /var/run/sshd && \
#    echo 'root:password' | chpasswd && \
#    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
#RUN service ssh start
#EXPOSE 22
#RUN /usr/sbin/sshd
#################################################
ARG CACHEBUST=1
# Add script path to PATH
#USER $USER_ID
ENV PATH="$PATH:/app/script"
ENTRYPOINT ["sleep", "infinity"]
