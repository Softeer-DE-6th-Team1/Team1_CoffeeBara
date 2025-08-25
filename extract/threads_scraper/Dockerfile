FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

WORKDIR /var/task

# Lambda 실행에 필요한 system dependencies 추가
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libnss3 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    libdrm2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libxrandr2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libpango1.0-0 \
    libgtk-3-0 \
    fonts-liberation \
    fontconfig && \
    rm -rf /var/lib/apt/lists/*

# Python dependencies 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir awslambdaric

# 코드 복사
COPY . .

# 캐시 디렉토리 설정 (Lambda /tmp 활용)
ENV XDG_CACHE_HOME=/tmp
ENV FONTCONFIG_PATH=/etc/fonts

# Lambda Entrypoint
CMD ["python3", "-m", "awslambdaric", "lambda_function.lambda_handler"]