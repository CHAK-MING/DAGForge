# syntax=docker/dockerfile:1.7

FROM ubuntu:25.04 AS builder

ARG BUILD_WEB_UI=0
ARG BUILD2_VERSION=0.17.0
ARG BUILD2_REPO_FINGERPRINT=70:64:FE:E4:E0:F3:60:F1:B4:51:E1:FA:12:5C:E0:B3:DB:DF:96:33:39:B9:2E:E5:C2:68:63:4C:A6:47:39:43
ARG BUILD_JOBS=0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gcc-14 \
    g++-14 \
    git \
    ccache \
    libboost-all-dev \
    libssl-dev \
    pkg-config \
    liburing-dev \
    nodejs \
    npm \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

ENV CCACHE_DIR=/root/.cache/ccache
ENV CCACHE_TEMPDIR=/tmp/ccache-tmp
ENV CCACHE_MAXSIZE=5G
ENV CCACHE_COMPILERCHECK=content
ENV CC=gcc-14
ENV CXX=g++-14
ENV PATH=/usr/local/bin:/root/.local/bin:${PATH}

RUN curl -fsSLO "https://download.build2.org/${BUILD2_VERSION}/build2-install-${BUILD2_VERSION}.sh" \
    && sh "build2-install-${BUILD2_VERSION}.sh" \
         --yes \
         --sudo false \
         --no-modules \
         --trust "${BUILD2_REPO_FINGERPRINT}" \
    && rm -f "build2-install-${BUILD2_VERSION}.sh"

WORKDIR /src

COPY README.md manifest repositories.manifest buildfile ./
COPY bin ./bin
COPY include ./include
COPY src ./src
COPY tests ./tests
COPY third_party ./third_party
COPY web-ui ./web-ui
COPY dags ./dags
COPY system_config.toml ./system_config.toml

RUN mkdir -p "${CCACHE_TEMPDIR}" "${CCACHE_DIR}"

RUN bdep init -C /tmp/dagforge-build2 @docker-build cc config.cxx="${CXX}"

RUN if [ "${BUILD_JOBS}" != "0" ]; then \
      bdep update @docker-build -j "${BUILD_JOBS}" 'bin/exe{dagforge}'; \
    else \
      bdep update @docker-build 'bin/exe{dagforge}'; \
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
    libboost-all-dev \
    libssl3 \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagforge

COPY --from=builder /tmp/dagforge-build2/dagforge/bin/dagforge /opt/dagforge/bin/dagforge
COPY --from=builder /src/system_config.toml /opt/dagforge/system_config.toml
COPY --from=builder /src/dags /opt/dagforge/dags
COPY --from=builder /src/web-ui/dist /opt/dagforge/web-ui-dist

EXPOSE 8888

CMD ["/opt/dagforge/bin/dagforge", "serve", "-c", "/opt/dagforge/system_config.toml"]
