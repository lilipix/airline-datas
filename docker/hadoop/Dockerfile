FROM ubuntu:26.04

ARG TARGETARCH
ARG HADOOP_VERSION=3.5.0

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17 \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_COMMON_LIB_NATIVE_DIR=/opt/hadoop/lib/native \
    LD_LIBRARY_PATH=/opt/hadoop/lib/native \
    HADOOP_OPTS="-Djava.library.path=/opt/hadoop/lib/native -XX:+IgnoreUnrecognizedVMOptions -XX:UseSVE=0" \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root \
    PATH="/opt/hadoop/bin:/opt/hadoop/sbin:/usr/lib/jvm/java-17/bin:${PATH}"

RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        ca-certificates \
        curl \
        procps \
        zlib1g-dev \
        libbz2-dev \
        libsnappy-dev \
        libssl-dev \
        libzstd1 \
        liblz4-1 \
    && rm -rf /var/lib/apt/lists/*; \
    ln -snf "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/java-17; \
    ARCH="${TARGETARCH:-$(uname -m)}"; \
    case "$ARCH" in \
        amd64|x86_64) HADOOP_ARCH="" ;; \
        arm64|aarch64) HADOOP_ARCH="-aarch64" ;; \
        *) echo "Architecture non supportée : $ARCH" >&2; exit 1 ;; \
    esac; \
    HADOOP_TARBALL="hadoop-${HADOOP_VERSION}${HADOOP_ARCH}.tar.gz"; \
    mkdir -p /opt/hadoop /data; \
    (curl -fsSL --retry 3 "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TARBALL}" \
     || curl -fsSL --retry 3 "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TARBALL}") \
    | tar -xz --strip-components=1 -C /opt/hadoop; \
    rm -rf /opt/hadoop/share/doc; \
    hdfs version

WORKDIR /opt/hadoop
