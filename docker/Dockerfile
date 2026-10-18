# PROPOSED. Spark + Delta + the streamwright-spark package. The jars are verified against Maven Central's published .sha1
# files at build time.
# BASE needs Python >= 3.10 (streamwright requires it). `apache/spark:3.5.3-python3` is Python 3.8 and fails; the Java 17
# Ubuntu image is used instead. The digest is the manifest-list digest Docker Hub reports for this tag (2024-09-27); the
# Python version inside is checked below at build time, because the tag listing does not state it.
ARG BASE=apache/spark:3.5.3-scala2.12-java17-python3-ubuntu@sha256:a1f2c9dcecb36c0f5c342be340b368883cfaa1d7ab23e6e222c8a93157b73a74
FROM ${BASE}

USER root
# fail fast, before any download, with a clear message
RUN python3 -c "import sys; print('python', sys.version.split()[0]); sys.exit(0 if sys.version_info >= (3, 10) else 'BASE has Python %d.%d; streamwright needs >= 3.10: choose another BASE' % sys.version_info[:2])" \
 && (python3 -m pip --version || (apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*))

ARG SPARK_VERSION=3.5.3
ARG DELTA_VERSION=3.2.1
ARG PG_JDBC_VERSION=42.7.4
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    cd /opt/spark/jars; \
    fetch() { \
      url="$1"; file="$(basename "$url")"; \
      curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 -o "$file" "$url"; \
      echo "$(curl --fail --silent --show-error --proto '=https' --tlsv1.2 "$url.sha1")  $file" | sha1sum -c -; \
    }; \
    fetch "$MAVEN/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar"; \
    fetch "$MAVEN/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"; \
    fetch "$MAVEN/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"; \
    fetch "$MAVEN/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    fetch "$MAVEN/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# StreamWright core and the connectors (the same ones `streamwright connectors install` gives). Until kafka and sqs have a
# hub release tag they come from the repo: compose passes it as the build context `streamwright` (additional_contexts).
ARG CONNECTORS="files s3 gcs azure_blob postgres deltalake kafka sqs"
COPY --from=streamwright core /tmp/sw/core
COPY --from=streamwright connectors/readers /tmp/sw/connectors
# Ubuntu 22.04's pip/setuptools (22.x / 59.x) cannot read PEP 621 `[project]` metadata (every package shows as
# "unknown 0.0.0" and pip reports a conflict), so upgrade the build tooling first. Stale build/, dist/ and *.egg-info
# folders in the repo copy are removed so nothing is built from them.
RUN set -eux; \
    python3 -m pip install --no-cache-dir --upgrade "pip>=24" "setuptools>=70" wheel; \
    python3 -m pip --version; python3 -c "import setuptools; print('setuptools', setuptools.__version__)"; \
    find /tmp/sw \( -name build -o -name dist -o -name '*.egg-info' \) -prune -exec rm -rf {} +; \
    python3 -m pip install --no-cache-dir /tmp/sw/core $(for c in $CONNECTORS; do echo /tmp/sw/connectors/$c; done); \
    rm -rf /tmp/sw

WORKDIR /opt/streamwright-spark
COPY pyproject.toml README.md ./
COPY src ./src
# the pip pyspark must match the Spark in /opt/spark (the jars above are on its classpath): constrain it to SPARK_VERSION
RUN set -eux; echo "pyspark==${SPARK_VERSION}" > /tmp/constraints.txt; \
    python3 -m pip install --no-cache-dir -c /tmp/constraints.txt ".[all]"

# no credentials are baked in: they arrive as STREAMWRIGHT_SECRET_* variables at run time
USER 185
WORKDIR /work
HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD ["python3", "-c", "import streamwright.spark"]
ENTRYPOINT ["sw-spark"]
