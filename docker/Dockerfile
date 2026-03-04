FROM ubuntu:22.04

ARG GO_VERSION=1.23.6

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Shanghai \
    GO111MODULE=on \
    CGO_ENABLED=1 \
    GOROOT=/usr/local/go \
    GOPATH=/go \
    PATH=/usr/local/go/bin:/go/bin:${PATH}

# 只安装 Go 基础开发环境
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates wget git tzdata \
    && rm -rf /var/lib/apt/lists/*

# 安装 Go 1.23
RUN wget -q https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz -O /tmp/go.tgz \
    && rm -rf /usr/local/go \
    && tar -C /usr/local -xzf /tmp/go.tgz \
    && rm -f /tmp/go.tgz

WORKDIR /home/likegochat

CMD ["bash"]
