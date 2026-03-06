# 1. 显式获取 GOPATH
GOPATH:=$(shell go env GOPATH)

# 2. 将 GOPATH/bin 加入到当前 Makefile 运行时的 PATH 变量中
export PATH:=$(GOPATH)/bin:$(PATH)

.PHONY: proto
proto: proto/auth.proto
	mkdir -p internal/common/proto/authpb
	protoc -I proto \
	  --go_out=internal/common/proto/authpb --go_opt=paths=source_relative \
	  --go-grpc_out=internal/common/proto/authpb --go-grpc_opt=paths=source_relative \
	  proto/auth.proto