# CI runner image. Build context is the repository root (wrangler.jsonc sets
# build_context ".."), so the image warms its caches from this tree:
# crates, the wasm and test builds, npm, worker-build's tools, the JRE and
# tla2tools.jar. A run then needs the network only for the git clone, plus
# crates.io or npm when a pull request changes a lockfile.
# linux/amd64 is required by Containers.
FROM --platform=linux/amd64 rust:1.99.1-slim-bookworm@sha256:f2c004e3d2d8b547e8cf7ffbc29205385f4e71d23e53e4e383065db5e9f822cd

ARG NODE_VERSION=22.23.4
ARG WORKER_BUILD_VERSION=0.8.7

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl git python3 xz-utils util-linux procps \
 && rm -rf /var/lib/apt/lists/*

# Node from the official tarball, checked against the release SHASUMS.
RUN set -eu; cd /tmp; \
    f="node-v${NODE_VERSION}-linux-x64.tar.xz"; \
    curl -fsSLO "https://nodejs.org/dist/v${NODE_VERSION}/${f}"; \
    curl -fsSLO "https://nodejs.org/dist/v${NODE_VERSION}/SHASUMS256.txt"; \
    grep " ${f}\$" SHASUMS256.txt | sha256sum -c -; \
    tar -xJf "$f" -C /usr/local --strip-components=1 --no-same-owner; \
    rm -f "$f" SHASUMS256.txt; node --version

# Toolchain and worker-build stay root-owned under /usr/local.
RUN rustup target add wasm32-unknown-unknown \
 && cargo install worker-build --version "${WORKER_BUILD_VERSION}" --locked \
 && rm -rf /usr/local/cargo/registry /usr/local/cargo/git

# The gates run as an unprivileged user. Root keeps the run nonce.
# Writable by ci at run time: CARGO_HOME (crate registry), the target dir,
# the npm cache, ~/.config, /work. Root-owned after the warm-up below:
# the JRE and tla2tools.jar, worker-build's tool cache (XDG_CACHE_HOME),
# the toolchain, and /opt/ci. Code under test then cannot swap a tool
# binary, for example a fake java that prints a TLC pass.
RUN useradd --create-home --uid 1000 ci \
 && mkdir -p /opt/warm /opt/tla-cache /opt/tools-cache /opt/ci /work \
 && chown -R ci:ci /opt/warm /opt/tla-cache /opt/tools-cache /work

ENV CARGO_HOME=/home/ci/.cargo \
    CARGO_TARGET_DIR=/opt/warm/target \
    XDG_CACHE_HOME=/opt/tools-cache \
    GE_TLA_CACHE=/opt/tla-cache \
    WRANGLER_SEND_METRICS=false \
    CLOUDFLARE_CF_FETCH_ENABLED=false \
    NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt

USER ci
WORKDIR /opt/warm

# JRE and tla2tools.jar: same URLs and sha256 as verify.sh, read from it so
# the two cannot drift.
COPY --chown=ci:ci verification/tla/verify.sh /opt/warm/verify.sh
RUN set -eu; export HOME=/home/ci; \
    v() { sed -nE "s/^$1=\"([^\"]+)\"/\\1/p" /opt/warm/verify.sh; }; \
    jre_dir="$(v JRE_DIR | sed 's#\$CACHE/##')"; \
    curl -fsSL "$(v TOOLS_URL)" -o /opt/tla-cache/tla2tools.jar; \
    echo "$(v TOOLS_SHA256)  /opt/tla-cache/tla2tools.jar" | sha256sum -c -; \
    curl -fsSL "$(v JRE_URL)" -o /opt/tla-cache/jre.tgz; \
    echo "$(v JRE_SHA256)  /opt/tla-cache/jre.tgz" | sha256sum -c -; \
    tar -xzf /opt/tla-cache/jre.tgz -C /opt/tla-cache; \
    test -x "/opt/tla-cache/${jre_dir}/bin/java"

# Warm the Rust and npm caches with this tree's server/: the crates, the
# test build, and one wasm build (which also fetches worker-build's
# wasm-bindgen, wasm-opt and esbuild into $XDG_CACHE_HOME/worker-build).
COPY --chown=ci:ci server/ /opt/warm/server/
RUN set -eu; export HOME=/home/ci; cd /opt/warm/server; \
    npm ci --no-audit --no-fund; \
    cargo test --locked --no-run; \
    worker-build --release; \
    rm -rf /opt/warm/server

COPY ci/gates.sh /opt/ci/gates.sh
COPY ci/container/entry.sh /opt/ci/entry.sh

USER root
# The base image makes CARGO_HOME and RUSTUP_HOME world-writable; undo that.
RUN chown -R root:root /opt/tla-cache /opt/tools-cache /opt/ci /usr/local/cargo /usr/local/rustup \
 && chmod -R go-w /opt/tla-cache /opt/tools-cache /opt/ci /usr/local/cargo /usr/local/rustup \
 && chmod 755 /opt/ci/gates.sh /opt/ci/entry.sh \
 && chmod 700 /root

# The locked-down tools must still run as ci.
USER ci
RUN set -eu; export HOME=/home/ci; \
    cargo --version; rustc --version; worker-build --version; node --version; \
    /opt/tla-cache/jdk-*/bin/java -version; \
    for d in /opt/tla-cache /opt/tools-cache /opt/ci /usr/local/cargo/bin /usr/local/rustup; do \
      if touch "$d/.w" 2>/dev/null; then echo "$d is writable by ci" >&2; exit 1; fi; \
    done

USER root
WORKDIR /work
ENTRYPOINT ["/opt/ci/entry.sh"]
