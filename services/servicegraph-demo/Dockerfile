FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/servicegraph-demo/pyproject.toml ./
COPY services/servicegraph-demo/src ./src

RUN pip install --no-cache-dir . \
    && groupadd --gid 10001 demo \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin demo

USER 10001:10001

ENTRYPOINT ["servicegraph-demo"]
