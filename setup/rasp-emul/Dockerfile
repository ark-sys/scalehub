FROM python:bookworm

# Define QEMU version as a build argument with a default value
ARG QEMU_VERSION=9.0.1

# This image downloads and install qemu from source https://download.qemu.org/qemu-9.0.1.tar.xz
RUN apt-get update && apt-get install -y \
    git \
    libglib2.0-dev \
    libfdt-dev \
    libpixman-1-dev \
    zlib1g-dev \
    ninja-build \
    git-email \
    libaio-dev \
    build-essential \
    gcc \
    wget \
    flex \
    bison \
    libbluetooth-dev \
    libcapstone-dev \
    libbrlapi-dev \
    libbz2-dev \
    libcap-ng-dev \
    libcurl4-gnutls-dev \
    libgtk-3-dev \
    libibverbs-dev \
    libjpeg62-turbo-dev \
    libncurses5-dev \
    libnuma-dev \
    librbd-dev \
    librdmacm-dev \
    libsasl2-dev \
    libsdl2-dev \
    libseccomp-dev \
    libsnappy-dev \
    libssh-dev \
    libvde-dev \
    libvdeplug-dev \
    libvte-2.91-dev \
    libxen-dev \
    liblzo2-dev \
    valgrind \
    xfslibs-dev

# Download and install qemu source
RUN wget https://download.qemu.org/qemu-${QEMU_VERSION}.tar.xz && \
    tar -xvf qemu-${QEMU_VERSION}.tar.xz && \
    cd qemu-${QEMU_VERSION} && \
    ./configure --enable-slirp && \
    make && \
    make install

# Clean raw sources
RUN rm -rf qemu-${QEMU_VERSION}.tar.xz qemu-${QEMU_VERSION}

# Setup environment variables for qemu configuration
ENV CPU_CORES=4 \
    RAM_SIZE=4G \
    DISK_SIZE=20G

# Stupid fix for missing shared library
RUN cp /usr/local/lib/x86_64-linux-gnu/libslirp.so.0 /lib/x86_64-linux-gnu/libslirp.so.0


# Start QEMU when the Docker container is started
#CMD qemu-system-aarch64 \
#    -M raspi4b \
#    -cpu cortex-a72 \
#    -m $RAM_SIZE -smp $CPU_CORES \
#    -nographic \
#    -kernel /mnt/kernel \
#    -append "rw earlyprintk loglevel=8 console=ttyAMA0,115200 dwc_otg.lpm_enable=0 root=/dev/vda2" \
#    -drive file=/mnt/raspos.img,format=raw,if=virtio \
#    -device usb-net,netdev=net0  \
#    -netdev user,id=net0,hostfwd=tcp::2222-:22,hostfwd=tcp::8086-:8086


## Hang container
ENTRYPOINT ["tail", "-f", "/dev/null"]
