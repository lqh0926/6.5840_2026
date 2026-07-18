# syntax=docker/dockerfile:1

# Build stage: this image contains the Go toolchain and source code, but none of
# it is copied into the final runtime image. Go 1.25 is required by the current
# gRPC/x packages even though src/go.mod still declares the module language at 1.22.
FROM golang:1.25.12-bookworm AS build

WORKDIR /workspace/src

# Copy dependency metadata first. Docker can reuse the go mod download layer
# while application source changes, as long as go.mod/go.sum stay unchanged.
COPY src/go.mod src/go.sum ./
# The course module intentionally keeps "go 1.22". The container dependency
# graph now requires Go 1.25.8, so only the builder's private copy is adjusted.
RUN go mod edit -go=1.25.8 \
    && go mod download

COPY src/ ./

# Pebble and the server are pure Go, so disabling CGO produces a static Linux
# binary that does not need libc in the runtime image.
RUN CGO_ENABLED=0 GOOS=linux go build -mod=mod \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/raftkvd \
    ./cmd/raftkvd \
    && mkdir -p /out/data

# Runtime stage: distroless has no shell, package manager, compiler, or source.
# The nonroot tag provides UID/GID 65532, which will also own the data directory.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

COPY --from=build --chown=65532:65532 /out/raftkvd /usr/local/bin/raftkvd
COPY --from=build --chown=65532:65532 /out/data/ /var/lib/raftkv/

ENV LISTEN=:7000 \
    PEER_PORT=7000 \
    CLIENT_PORT=7001 \
    DATA_DIR=/var/lib/raftkv

WORKDIR /var/lib/raftkv
USER 65532:65532

# EXPOSE documents the intended ports; publishing/routing them is done by
# docker run, Compose, or Kubernetes rather than by this instruction itself.
EXPOSE 7000 7001

STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/raftkvd"]
