FROM ubuntu:26.04

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_INSTALL_DIR=/opt/python \
    VIRTUAL_ENV=/opt/venv \
    HADOOP_USER_NAME=root \
    JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_CONF_DIR=/opt/spark/conf \
    PATH="/opt/venv/bin:/usr/lib/jvm/default-java/bin:${PATH}" \
    PYSPARK_PYTHON=/usr/local/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    ca-certificates \
    libsnappy1v5 \
    git \
    jq \
    procps \
    htop \
    curl \
    less \
    nano \
    bash-completion \
    iputils-ping \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && ln -snf "/usr/lib/jvm/java-21-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/default-java \
    && mkdir -p /opt/spark/conf \
    && git config --system --add safe.directory /workspace

RUN uv python install 3.15 && \
    uv venv /opt/venv --python 3.15 && \
    ln -snf /opt/venv/bin/python /usr/local/bin/python && \
    ln -snf /opt/venv/bin/python3 /usr/local/bin/python3

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --no-cache -r /tmp/requirements.txt && \
    python3 -m ipykernel install --prefix /usr/local --name python3 --display-name "Python 3.15 (PySpark 4.2 + Delta Lake 4.4)" && \
    python3 -c "import shutil; from pyspark.sql import SparkSession; \
    spark = SparkSession.builder \
    .master('local[1]') \
    .config('spark.driver.extraJavaOptions', '-XX:+IgnoreUnrecognizedVMOptions -XX:UseSVE=0') \
    .config('spark.jars.packages', 'io.delta:delta-spark_4.2_2.13:4.4.1') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate(); \
    spark.createDataFrame([(1, 'ok')], ['id', 'status']).write.format('delta').mode('overwrite').save('/tmp/delta_smoke'); \
    assert spark.read.format('delta').load('/tmp/delta_smoke').toPandas().shape == (1, 2); \
    spark.stop(); \
    shutil.rmtree('/tmp/delta_smoke', ignore_errors=True)"

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    unzip \
    libatomic1 \
    && rm -rf /var/lib/apt/lists/*

# Métadonnées Dev Container intégrées à l'image (actives aussi via "Attach Visual Studio Code" sur spark-jupyter)
LABEL devcontainer.metadata='[{"customizations":{"vscode":{"extensions":["ms-python.python","ms-python.vscode-pylance","ms-python.debugpy","charliermarsh.ruff","ms-toolsai.jupyter","ms-toolsai.jupyter-keymap","ms-toolsai.jupyter-renderers","mechatroner.rainbow-csv","redhat.vscode-xml","redhat.vscode-yaml"],"settings":{"python.defaultInterpreterPath":"/usr/local/bin/python3","jupyter.kernels.trusted":["/usr/local/share/jupyter/kernels/python3/kernel.json"]}}}}]'
