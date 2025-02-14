FROM alpine:3.21.2

RUN apk --no-cache add dnsmasq

ADD dnsmasq.conf /etc/dnsmasq.conf

EXPOSE 53 53/udp

ENTRYPOINT ["dnsmasq", "-k"]