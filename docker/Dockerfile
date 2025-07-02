FROM alpine:latest

#VOLUME ["awesome","/root/awz"]

# 安装运行时依赖
RUN apk --no-cache add tzdata

# 设置工作目录
WORKDIR /root/

# 从构建阶段复制编译好的应用
COPY awesome ./

# 复制必要的配置文件
COPY .env ./
COPY clash.yaml ./
COPY Country.mmdb ./
# 设置时区
ENV TZ=Asia/Shanghai

# 设置容器启动命令
ENTRYPOINT ["/root/awesome"]

# 默认参数，可以被docker run命令行参数覆盖
CMD ["--id", "default_task", "--type", "search_products", "--keyword", "example", "--max", "1", "--code", "US"]