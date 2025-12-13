.PHONY: build-web build-server build-servers build-release clean

# 变量定义
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -s -w -X github.com/dushixiang/uart_sms_forwarder/internal/version.Version=$(VERSION)
GOFLAGS := CGO_ENABLED=0

# 构建前端
build-web:
	@echo "Building web frontend..."
	cd web && yarn && yarn build
	@echo "Web frontend built successfully!"

# 构建服务端（单平台 - Linux amd64）
build-server:
	@echo "Building server for Linux amd64..."
	@mkdir -p bin
	$(GOFLAGS) GOOS=linux GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o bin/uart_sms_forwarder-linux-amd64 cmd/serv/main.go
	upx bin/uart_sms_forwarder-linux-amd64
	@echo "Server built successfully!"
	@ls -lh bin/

# 构建所有（发布版本）
build-release:
	@echo "Building release version..."
	make build-web
	make build-server
	@echo "Release build completed!"

# 清理构建文件
clean:
	@echo "Cleaning build files..."
	rm -rf bin
	rm -rf web/dist
	@echo "Clean completed!"

# 开发构建（不包含前端，不压缩）
dev:
	@echo "Building for development..."
	@mkdir -p bin
	go build -o bin/uart_sms_forwarder cmd/serv/main.go
	@echo "Development build completed!"
	@ls -lh bin/

# 运行（开发模式）
run:
	@echo "Running in development mode..."
	go run cmd/serv/main.go

# 默认目标
build: build-release
