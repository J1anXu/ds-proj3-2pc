FROM python:3.11-slim

WORKDIR /app

# ---- 安装系统依赖 ----
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# ---- 复制依赖文件并安装 ----
COPY ./web/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ---- 复制应用代码 ----
COPY ./web /app

# ---- 暴露端口 ----
EXPOSE 5000
EXPOSE 5001

# 默认不运行（docker-compose 中会覆盖）
CMD ["python", "app.py"]
