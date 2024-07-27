# Docker image definition containing OpenVPN client and OpenConnect client

FROM ubuntu:20.04

RUN apt-get update && apt-get install -y openvpn openconnect curl iptables tini tzdata && \
    addgroup --system vpn && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY openvpn.sh /usr/bin/

RUN chmod +x /usr/bin/openvpn.sh

HEALTHCHECK --interval=60s --timeout=15s --start-period=120s --retries=3 CMD curl -LSs 'https://api.ipify.org'

VOLUME ["/vpn"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/bin/openvpn.sh"]