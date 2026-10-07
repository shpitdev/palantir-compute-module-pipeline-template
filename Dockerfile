# syntax=docker/dockerfile:1

FROM --platform=linux/amd64 golang:1.26.8 AS builder

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/enricher ./cmd/enricher

FROM debian:bookworm-slim

# Compute Modules can capture stdout logs by wrapping the entrypoint with `/bin/sh` and `tee`.
# Distroless images do not ship with a shell, so we use a minimal Debian base and install CA certs
# for outbound HTTPS (Gemini) at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/enricher /enricher

# Foundry requires the image user to be numeric and non-root.
# Many Foundry-mounted secret/token files are readable by uid 5000.
USER 5000:5000

ENTRYPOINT ["/enricher", "foundry"]
