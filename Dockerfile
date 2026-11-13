FROM golang:1.27.4-alpine AS builder
WORKDIR /app
RUN apk --no-cache add ca-certificates tzdata && mkdir -m 1777 -p /rootfs/tmp

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build \
    -ldflags="-w -s" \
    -a \
    -installsuffix cgo \
    -o datalogger ./cmd/datalogger


FROM scratch
WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /app/datalogger /app
# /tmp holds the status socket; 1777 so the image runs as root or any user (compose user:).
COPY --from=builder /rootfs/ /
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/app/datalogger", "-healthcheck"]
ENTRYPOINT ["/app/datalogger"]
