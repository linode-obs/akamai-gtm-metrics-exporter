# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.24 AS builder

ARG ARCH
ARG OS

RUN apt-get update && apt-get install -y make git ca-certificates

WORKDIR /go/src/github.com/akamai/akamai-gtm-metrics-exporter
COPY . .

RUN CGO_ENABLED=0 GOOS=${OS} GOARCH=${ARCH} make build

FROM busybox:latest AS app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/src/github.com/akamai/akamai-gtm-metrics-exporter/akamai-gtm-metrics-exporter /bin/akamai-gtm-metrics-exporter

EXPOSE 9999
ENTRYPOINT ["/bin/akamai-gtm-metrics-exporter"]
