#!/bin/bash

# SSH 키 생성
if [ ! -f ~/.ssh/id_rsa ]; then
  ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  chmod 700 ~/.ssh
  chmod 600 ~/.ssh/authorized_keys
fi

# Spark Worker 실행
$SPARK_HOME/sbin/start-worker.sh ${SPARK_MASTER} \
  --webui-port ${SPARK_WORKER_WEBUI_PORT}

# 컨테이너가 꺼지지 않도록 유지
tail -f /dev/null