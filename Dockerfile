FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/oap-backup .

FROM debian:trixie-slim
# CA certificates are needed for HTTPS (S3) and FTPS (--ftp_tls). Time zone
# data is embedded in the binary (time/tzdata), so tzdata is not installed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/oap-backup /oap-backup

# oap-backup needs no root: it only talks to the Kubernetes API and the
# backup storage, and writes temporary backup files to --tmp_dir (/tmp).
USER 65532:65532
EXPOSE 9090

ENTRYPOINT ["/oap-backup"]
