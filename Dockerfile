# turnhive server image.
#
# The build is a single stage: the binary is compiled on the host (Go
# 1.27, CGO disabled) and copied in, so the image build needs no network
# beyond the base image. Build from the repository root:
#
#   CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o build/turnhive ./cmd/turnhive
#   docker build -t 127.0.0.1:5000/turnhive:dev .
#
# The base image is the same ubuntu the local ironhive sandboxes use; any
# minimal base with CA certificates works.

FROM 127.0.0.1:5000/ubuntu:26.04

# CA certificates for outbound HTTPS (the LLM endpoint); the ubuntu base
# keeps them out of the default install set.
COPY build/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY build/turnhive /usr/bin/turnhive

EXPOSE 8080
ENTRYPOINT ["/usr/bin/turnhive"]
CMD ["-config", "/etc/turnhive/config.yml"]
