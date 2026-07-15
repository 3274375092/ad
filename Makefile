.PHONY: help tidy build run-server run-consumer run-producer init-ch docker-up docker-down docker-logs clean test test-unit test-integration test-coverage bench-batch bench-query bench-all

BIN_DIR := bin

help:
	@echo "=== Ad Data Platform ==="
	@echo "make tidy           - 下载依赖"
	@echo "make build          - 编译所有二进制"
	@echo "make run-server     - 启动 API 服务"
	@echo "make run-consumer   - 启动消费者"
	@echo "make run-producer   - 启动数据生成器"
	@echo "make init-ch        - 初始化 ClickHouse 表"
	@echo "make docker-up      - docker-compose 启动全部"
	@echo "make docker-down    - docker-compose 关闭"
	@echo "make docker-logs    - 查看 docker 日志"
	@echo "make test           - 运行所有单元测试"
	@echo "make test-unit      - 仅运行单元测试"
	@echo "make test-integration - 运行集成测试（需要 CH）"
	@echo "make test-coverage  - 运行测试并生成覆盖率报告"
	@echo "make clean          - 清理二进制 + 覆盖率报告"

tidy:
	go mod tidy

build:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -o $(BIN_DIR)/server.exe ./cmd/server
	CGO_ENABLED=0 go build -o $(BIN_DIR)/consumer.exe ./cmd/consumer
	CGO_ENABLED=0 go build -o $(BIN_DIR)/producer.exe ./cmd/producer
	@echo "Built: $(BIN_DIR)/{server,consumer,producer}.exe"

run-server:
	go run ./cmd/server -config ./configs/config.yaml

run-consumer:
	go run ./cmd/consumer -config ./configs/config.yaml

run-producer:
	go run ./cmd/producer -config ./configs/config.yaml -qps=200

init-ch:
	docker exec -i ad-clickhouse clickhouse-client < scripts/init_clickhouse.sql
	@echo "ClickHouse initialized."

docker-up:
	cd deploy && docker-compose up -d
	@echo "Waiting for ClickHouse..."
	@sleep 30
	@echo "All services started."

docker-down:
	cd deploy && docker-compose down

docker-logs:
	cd deploy && docker-compose logs -f --tail=100

# 单元测试（不需要真实 CH）
test-unit:
	go test -v -race -cover ./internal/...

# 集成测试（需要真实 CH 通过环境变量 INTEGRATION=1 启用）
test-integration:
	cd deploy && docker-compose up -d clickhouse kafka zookeeper
	@echo "Waiting for services..."
	@sleep 30
	cd ..
	docker exec -i ad-clickhouse clickhouse-client < scripts/init_clickhouse.sql || true
	INTEGRATION=1 go test -v -tags=integration ./internal/repository/...

# 默认 test 命令
test:
	go test -v -race ./internal/...

# 覆盖率报告
test-coverage:
	mkdir -p coverage
	go test -coverprofile=coverage/coverage.out ./internal/...
	go tool cover -html=coverage/coverage.out -o coverage/coverage.html
	go tool cover -func=coverage/coverage.out
	@echo ""
	@echo "Coverage report: coverage/coverage.html"

clean:
	rm -rf $(BIN_DIR)
	rm -rf coverage

# ============================================================
# Benchmarks（需要 ClickHouse 运行且已灌入数据）
# ============================================================

# 写入吞吐基准测试
bench-batch:
	INTEGRATION=1 go test -v -tags=integration -bench=BenchmarkIntegration_BatchInsert -benchmem -benchtime=5s ./internal/repository/...

# 查询性能基准测试
bench-query:
	INTEGRATION=1 go test -v -tags=integration -bench=BenchmarkIntegration_RealtimeOverview -benchmem -benchtime=3s ./internal/repository/...

# 全部 benchmark
bench-all:
	INTEGRATION=1 go test -v -tags=integration -bench=. -benchmem -benchtime=3s ./internal/repository/...