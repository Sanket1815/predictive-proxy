# syntax=docker/dockerfile:1
# Multi-stage build — final image is based on scratch (zero OS overhead).
# The binary is statically linked; only the TLS root certificates and the
# compiled eBPF object are included alongside it.

# ── Stage 1: eBPF compilation ───────────────────────────────────────────────
FROM ubuntu:22.04 AS bpf-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang-14 llvm-14 libbpf-dev linux-headers-generic \
    && ln -sf /usr/bin/clang-14 /usr/local/bin/clang \
    && ln -sf /usr/bin/llvm-strip-14 /usr/local/bin/llvm-strip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/ebpf
COPY ebpf/tc_tracker.c .

RUN clang -O2 -g -target bpf -D__TARGET_ARCH_x86 \
        -I/usr/include/x86_64-linux-gnu \
        -c tc_tracker.c -o tc_tracker.bpf.o \
    && llvm-strip -g tc_tracker.bpf.o

# ── Stage 2: Go compilation ──────────────────────────────────────────────────
FROM golang:1.22-alpine AS go-builder

# CGO is disabled for a fully static binary; the AWS SDK and cilium/ebpf are
# pure-Go so no C dependencies exist at runtime.
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64

WORKDIR /src

# Cache the dependency download layer separately from the source layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Copy the compiled eBPF object so the go:embed directive in bpf_bpfel.go
# can find it during the build.
COPY --from=bpf-builder /src/ebpf/tc_tracker.bpf.o ./ebpf/

RUN go build -trimpath -ldflags="-s -w" -o /bin/proxy ./cmd/proxy

# ── Stage 3: Minimal runtime image ──────────────────────────────────────────
FROM scratch

# TLS root certificates — required for HTTPS connections to Wasabi/S3.
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# The proxy binary.
COPY --from=go-builder /bin/proxy /proxy

# Default configuration. Override by bind-mounting your own config at runtime:
#   docker run -v $(pwd)/configs:/configs ghcr.io/nadsanket7/go-predictive-proxy
COPY configs/proxy.config.yaml /configs/proxy.config.yaml

# Proxy port and metrics port.
EXPOSE 8080 9090

# Run as a non-root UID to satisfy container security policies.
# scratch has no /etc/passwd; numeric UID is used directly.
USER 65534:65534

ENTRYPOINT ["/proxy"]
CMD ["-config", "/configs/proxy.config.yaml"]
