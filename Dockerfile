FROM alpine:latest
COPY target/x86_64-unknown-linux-musl/release/check-backups target/x86_64-unknown-linux-musl/release/check-backups ./
