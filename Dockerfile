FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/postgresql_to_bigquery.py"

COPY requirements.txt /template/

# We could get rid of installing libffi-dev and git, or we could leave them.
RUN apt-get update \
    && apt-get install -y openjdk-11-jdk libffi-dev git \
    && rm -rf /var/lib/apt/lists/* \
    # Upgrade pip and install the requirements.
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    # Download the requirements to speed up launching the Dataflow job.
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Since we already downloaded all the dependencies, there's no need to rebuild everything.
ENV PIP_NO_DEPS=True

COPY src/postgresql_to_bigquery.py /template/

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]