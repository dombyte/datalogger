FROM golang:1.26.7-alpine AS builder
WORKDIR /app
RUN apk --no-cache add ca-certificates tzdata

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build \
    -ldflags="-w -s" \
    -a \
    -installsuffix cgo \
    -o datalogger ./main.go


FROM scratch
WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /app/datalogger /app
ENTRYPOINT ["/app/datalogger"]
