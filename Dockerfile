# syntax=docker/dockerfile:1.7

FROM ubuntu:25.04 AS builder

ARG BUILD_WEB_UI=0
ARG CMAKE_PRESET=release
ARG CMAKE_BUILD_PARALLEL_LEVEL=0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    cmake \
    gcc-14 \
    g++-14 \
    make \
    ninja-build \
    git \
    ccache \
    libboost-all-dev \
    libssl-dev \
    pkg-config \
    liburing-dev \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Boost CONFIG mode may split components (charconv/url/process)
# into versioned dev packages on Ubuntu. Install any available ones.
RUN set -eux; \
    apt-get update; \
    mapfile -t boost_component_pkgs < <(apt-cache pkgnames | grep -E '^libboost-(charconv|url|process).*-dev$' | sort -u || true); \
    if [ "${#boost_component_pkgs[@]}" -gt 0 ]; then \
      apt-get install -y --no-install-recommends "${boost_component_pkgs[@]}"; \
    fi; \
    rm -rf /var/lib/apt/lists/*

ENV CCACHE_DIR=/root/.cache/ccache
ENV CCACHE_TEMPDIR=/tmp/ccache-tmp
ENV CCACHE_MAXSIZE=5G
ENV CCACHE_COMPILERCHECK=content
ENV CC=gcc-14
ENV CXX=g++-14

WORKDIR /src

# Copy build metadata first for better Docker layer reuse.
COPY CMakeLists.txt CMakePresets.json ./
COPY cmake ./cmake
COPY include ./include
COPY src ./src
COPY web-ui ./web-ui
COPY dags ./dags
COPY system_config.toml ./system_config.toml

RUN mkdir -p "${CCACHE_TEMPDIR}" "${CCACHE_DIR}"

RUN cmake --preset "${CMAKE_PRESET}" \
    -G Ninja \
    -DDAGFORGE_ENABLE_TESTS=OFF \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DCMAKE_C_COMPILER_LAUNCHER=ccache

RUN if [ "${CMAKE_BUILD_PARALLEL_LEVEL}" != "0" ]; then \
      cmake --build --preset "${CMAKE_PRESET}" --target dagforge --parallel "${CMAKE_BUILD_PARALLEL_LEVEL}"; \
    else \
      cmake --build --preset "${CMAKE_PRESET}" --target dagforge; \
    fi

RUN if [ "${BUILD_WEB_UI}" = "1" ]; then \
      cd web-ui && (test -f package-lock.json && npm ci || npm install) && npm run build; \
    else \
      mkdir -p web-ui/dist; \
    fi

RUN ccache -s || true

FROM ubuntu:25.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    libboost-dev \
    libssl3 \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagforge

COPY --from=builder /src/build-release/bin/dagforge /opt/dagforge/bin/dagforge
COPY --from=builder /src/system_config.toml /opt/dagforge/system_config.toml
COPY --from=builder /src/dags /opt/dagforge/dags
COPY --from=builder /src/web-ui/dist /opt/dagforge/web-ui-dist

EXPOSE 8888

CMD ["/opt/dagforge/bin/dagforge", "serve", "-c", "/opt/dagforge/system_config.toml"]
