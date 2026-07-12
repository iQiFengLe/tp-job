# syntax=docker/dockerfile:1

# ---- stage 1: 构建前端管理台 ----
FROM node:20-alpine AS web
WORKDIR /web
COPY web/package.json ./
RUN npm install --no-audit --no-fund
COPY web/ ./
RUN npm run build
# 产物位于 /web/dist

# ---- stage 2: 构建后端 ----
FROM golang:1.26-alpine AS go
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /web/dist ./web/dist
# 纯 Go SQLite（glebarez/sqlite），CGO_ENABLED=0 产静态二进制，便于打入瘦镜像
RUN CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/dida .

# ---- stage 3: 运行 ----
FROM alpine:3.20
# ca-certificates: HTTPS webhook 触发需要；tzdata: cron 按本地时区计算
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 app
WORKDIR /app
COPY --from=go /out/dida ./
COPY config.yaml config.release.yaml ./
# 前端管理台已 //go:embed 编译进二进制，运行镜像无需单独携带 web/dist
RUN mkdir -p /app/data /app/logs && chown -R app:app /app
USER app
EXPOSE 8080
# /health 在 DB 不可达时返回 503，探针据此判定不可用
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --spider -q http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["./dida"]
CMD ["-config", "config.yaml"]
