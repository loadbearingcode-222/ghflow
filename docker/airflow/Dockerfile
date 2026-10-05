# Airflow 3 for the batch side: the stock image, plus ghflow in an ISOLATED venv (olist ADR-0007).
# Airflow's own dependencies stay exactly as the image ships them; tasks run the project's code
# with /opt/ghflow-venv, built from uv.lock like every other environment. Pins: ADR-0001.
FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

USER root
COPY --from=ghcr.io/astral-sh/uv:0.11.25@sha256:1e3808aa9023d0980e7c15b1fa7c1ac16ff35925780cf5c459858b2d693f01a9 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/ghflow-venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /opt/ghflow
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
COPY tests/dags ./tests/dags
RUN uv sync --locked --no-dev \
    && chown -R airflow:0 /opt/ghflow /opt/ghflow-venv

USER airflow
WORKDIR /opt/airflow
