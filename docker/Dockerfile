FROM ubuntu:22.04

ARG GO_VERSION=1.23.6
ARG ETCD_VERSION=v3.5.13
ARG KAFKA_VERSION=3.7.0
ARG SCALA_VERSION=2.13

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Shanghai \
    GO111MODULE=on \
    CGO_ENABLED=1 \
    GOROOT=/usr/local/go \
    GOPATH=/go \
    PATH=/usr/local/go/bin:/go/bin:/opt/kafka/bin:${PATH}

# 只安装：Go 开发工具 + 中间件运行环境
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl wget ca-certificates git make gcc g++ pkg-config tzdata \
    redis-server mysql-server openjdk-17-jre-headless supervisor \
    net-tools iproute2 procps \
    && rm -rf /var/lib/apt/lists/*

# 安装 Go
RUN wget -q https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz -O /tmp/go.tgz \
    && rm -rf /usr/local/go \
    && tar -C /usr/local -xzf /tmp/go.tgz \
    && rm -f /tmp/go.tgz

# 安装 etcd
RUN wget -qO- https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz \
    | tar xz -C /tmp \
    && mv /tmp/etcd-${ETCD_VERSION}-linux-amd64/etcd /usr/local/bin/etcd \
    && mv /tmp/etcd-${ETCD_VERSION}-linux-amd64/etcdctl /usr/local/bin/etcdctl \
    && rm -rf /tmp/etcd-${ETCD_VERSION}-linux-amd64

# 安装 Kafka（KRaft/ZK 均可后续自行配置）
RUN wget -q https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -O /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

WORKDIR /home/likegochat

EXPOSE 2379 3306 6379 9092

# 不自动启动任何服务，进入容器后手动配置
CMD ["bash"]
