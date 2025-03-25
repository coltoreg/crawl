# 第一階段：build stage（用完整的 Python 映像檔進行依賴安裝）
FROM --platform=linux/amd64 python:3.11-slim AS builder
WORKDIR /app

# 複製依賴描述檔
COPY requirements.txt .

# 建立虛擬環境並安裝依賴
RUN python -m venv /venv && \
    /venv/bin/pip install --upgrade pip && \
    /venv/bin/pip install --no-cache-dir -r requirements.txt

# 第二階段：production stage（使用較小的 slim 映像檔）
FROM --platform=linux/amd64 python:3.11-slim
WORKDIR /app

# 安裝 Playwright 依賴
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxcb1 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libxcursor1 \
    libgtk-3-0 \
    ca-certificates \
    fonts-noto-color-emoji \
    fonts-liberation \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 從 builder 階段複製已安裝依賴的虛擬環境
COPY --from=builder /venv /venv

# 複製應用程式代碼
COPY . /app

# 更新 PATH，確保使用虛擬環境內的 Python 與 pip
ENV PATH="/venv/bin:$PATH"

# 安裝 Playwright 瀏覽器
RUN playwright install chromium firefox webkit --with-deps

# 建立輸出目錄
RUN mkdir -p /app/output && chmod 777 /app/output

# 設定環境變數
ENV PYTHONUNBUFFERED=1
ENV DB_HOST=YOUR_RDS_ENDPOINT
ENV DB_USER=YOUR_RDS_USER
ENV DB_PASSWORD=YOUR_RDS_PASSWORD
ENV DB_DATABASE=crawl
ENV ES_HOST=http://elasticsearch:9200

# 啟動命令
CMD ["python", "main.py", "--init"]