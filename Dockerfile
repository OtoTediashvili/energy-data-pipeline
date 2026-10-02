# Airflow plus everything the pipeline needs, built once and shared by every
# Airflow service in docker-compose.yml.
#
# Code is not copied in: dags/, src/ and dbt/ are bind-mounted, so edits take
# effect without a rebuild. Only a dependency change in pyproject.toml needs
# one, and `make up` rebuilds automatically.

ARG AIRFLOW_VERSION=3.0.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

COPY pyproject.toml /tmp/pyproject.toml

# The pipeline's runtime dependencies, read from pyproject.toml so the image and
# the local venv share one list. Installed into Airflow's own environment against
# Airflow's published constraints, with Airflow itself pinned, so nothing Airflow
# relies on can be upgraded or downgraded underneath it.
RUN python -c "import tomllib; d = tomllib.load(open('/tmp/pyproject.toml', 'rb')); print('\n'.join(d['project']['dependencies']))" > /tmp/requirements.txt \
 && pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt gets a virtualenv of its own. dbt and Airflow pin conflicting versions of
# shared libraries; in separate environments neither can break the other.
USER root
RUN python -c "import tomllib; d = tomllib.load(open('/tmp/pyproject.toml', 'rb')); print('\n'.join(d['project']['optional-dependencies']['dbt']))" > /tmp/dbt-requirements.txt \
 && python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt
USER airflow
