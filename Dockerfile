# -------- 阶段1: 构建阶段 --------
FROM golang:1.25-alpine AS builder

# 启用Go Modules并下载依赖
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# 拷贝源码并构建可执行文件
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags="-w -s" -o /app/server ./app/

# -------- 阶段2: 运行阶段 --------
FROM scratch

# 设置工作目录
WORKDIR /app

# 从构建阶段拷贝编译产物
COPY --from=builder /app/server .

# 暴露端口（按你的服务端口改）
EXPOSE 6379

# 启动命令
CMD ["./server"]


