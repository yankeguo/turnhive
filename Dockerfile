FROM golang:1.27 AS builder
ENV CGO_ENABLED=0
WORKDIR /go/src/app
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /turnhive ./cmd/turnhive

FROM debian:bookworm-slim
# ca-certificates for outbound HTTPS (the LLM endpoint). The server spawns
# no child processes (sandbox execution is remote via ironhive), so no
# init/reaper is needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
	ca-certificates \
	&& rm -rf /var/lib/apt/lists/*
COPY --from=builder /turnhive /usr/bin/turnhive
EXPOSE 8080
ENTRYPOINT ["/usr/bin/turnhive"]
CMD ["-config", "/etc/turnhive/config.yml"]
