# syntax=docker/dockerfile:1

FROM --platform=linux/amd64 golang:1.25 AS builder

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/enricher ./cmd/enricher

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/enricher /enricher

# Foundry requires the image user to be numeric. Distroless nonroot maps to uid/gid 65532.
USER 65532:65532

ENTRYPOINT ["/enricher"]
CMD ["foundry"]
