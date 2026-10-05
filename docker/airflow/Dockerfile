# Airflow 3 for the batch side: the stock image, plus ghflow in an ISOLATED venv (olist ADR-0007).
# Airflow's own dependencies stay exactly as the image ships them; tasks run the project's code
# with /opt/ghflow-venv, built from uv.lock like every other environment. Pins: ADR-0001.
#
# Stages: `base` is the image; `dag-test` adds the DAG-integrity runner (pytest, pinned to the
# version in uv.lock) so the check runs with networking off; `runtime`, the last stage, is what
# compose builds and runs — it never carries the test runner.
FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1 AS base

USER root
COPY --from=ghcr.io/astral-sh/uv:0.12.23@sha256:61d393e44e249f2e4b526b6c7ddcecce245946826e608e11c93ad4f5bba55b21 /uv /usr/local/bin/uv
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

FROM base AS dag-test
# Airflow's own interpreter runs the DAG tests (they import airflow); pytest must match uv.lock.
RUN pip install --no-cache-dir "pytest==9.1.1"

FROM base AS runtime
