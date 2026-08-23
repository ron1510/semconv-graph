# syntax=docker/dockerfile:1.7
FROM python:3.14-slim-bookworm

ARG PIP_INDEX_URL
ARG PIP_TRUSTED_HOST

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build
COPY services/servicegraph-indexer ./services/servicegraph-indexer
RUN --mount=type=cache,target=/tmp/pip-cache \
    PIP_CACHE_DIR=/tmp/pip-cache python -m pip install \
      ./services/servicegraph-indexer

RUN groupadd --gid 10001 servicegraph \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin servicegraph

WORKDIR /app
USER 10001:10001
ENTRYPOINT ["servicegraph-indexer"]
