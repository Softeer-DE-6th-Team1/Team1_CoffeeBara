#!/bin/bash

# SSH 키 생성 
if [ ! -f ~/.ssh/id_rsa ]; then
  ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  chmod 700 ~/.ssh
  chmod 600 ~/.ssh/authorized_keys
fi

# Spark 이벤트 로그 디렉토리 (docker-compose에서 볼륨 마운트 예정)
LOG_DIR=${SPARK_HISTORY_LOG_DIR:-/home/softeer/spark-events}

# 디렉토리 없으면 생성
mkdir -p "$LOG_DIR"

# Spark History Server 실행
echo "Starting Spark History Server with logs at $LOG_DIR ..."
$SPARK_HOME/sbin/start-history-server.sh \
  --properties-file $SPARK_HOME/conf/spark-defaults.conf \
  -Dspark.history.fs.logDirectory=$LOG_DIR

# 컨테이너가 꺼지지 않도록 유지
tail -f /dev/null
