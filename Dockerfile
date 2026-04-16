# ---- Build stage ----
FROM golang:1.23-bookworm AS builder

WORKDIR /src

# Cache module downloads separately from source
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /jellyfin-cache .

# ---- Runtime stage ----
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /jellyfin-cache /usr/local/bin/jellyfin-cache

# Config and cache dirs; both are typically overridden by k8s mounts
RUN mkdir -p /etc/jellyfin-cache /etc/rclone /data/cache

EXPOSE 2049/tcp
EXPOSE 8089/tcp
EXPOSE 8090/tcp

ENTRYPOINT ["jellyfin-cache"]
CMD ["mount", "--config", "/etc/jellyfin-cache/config.yaml"]
