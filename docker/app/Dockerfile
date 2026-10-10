# One Python image for the stream services (M2-S1, ADR-0005): replayer now, lander (M2-S2) and the
# repo-state processor (M3-S2) later. ghflow and its stream group only; no Airflow. Pins: ADR-0001.
FROM python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2

COPY --from=ghcr.io/astral-sh/uv:0.12.23@sha256:61d393e44e249f2e4b526b6c7ddcecce245946826e608e11c93ad4f5bba55b21 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/ghflow-venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    PATH=/opt/ghflow-venv/bin:$PATH
WORKDIR /opt/ghflow
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --group stream --group clickhouse --no-install-project
COPY src ./src
COPY config/schemas ./config/schemas
RUN uv sync --locked --no-dev --group stream --group clickhouse \
    && useradd --uid 50001 --no-create-home ghflow \
    && mkdir -p /var/lib/ghflow-state \
    && chown -R ghflow /opt/ghflow /opt/ghflow-venv /var/lib/ghflow-state
USER ghflow
CMD ["python", "-m", "ghflow.commands.stream", "run"]
