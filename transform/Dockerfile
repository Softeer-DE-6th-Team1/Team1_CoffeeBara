FROM amazonlinux:2023

USER root

# os 패키지 설치
RUN dnf update -y && \
    dnf install -y java-17-amazon-corretto-devel python3 python3-pip \
    openssh-clients openssh-server pdsh unzip tar gzip procps-ng net-tools hostname rsync&&\
    dnf clean all

# Spark 3.5.1 설치
WORKDIR /opt
RUN curl -fSL -o spark.tgz https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark.tgz && \
    mv spark-3.5.1-bin-hadoop3 spark && \
    rm spark.tgz

# Python 패키지 설치
RUN pip3 install boto3 pandas numpy

# 환경변수 등록
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# S3A connector (Hadoop AWS + AWS SDK)
RUN curl -fSL -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fSL -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL -o /opt/spark/jars/spark-hadoop-cloud_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/3.5.1/spark-hadoop-cloud_2.12-3.5.1.jar 

# PostgreSQL driver
RUN curl -fSL -o $SPARK_HOME/jars/postgresql-42.7.4.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# spark conf
COPY conf/workers ${SPARK_HOME}/conf/workers
COPY conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

# user 생성 및 권한 변경
RUN useradd -m -s /bin/bash softeer && \
    echo "softeer ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    mkdir -p /home/softeer/.ssh && \
    chown -R softeer:softeer /home/softeer && \
    chmod 700 /home/softeer/.ssh

# python 환경변수 설정 (system-wide)
RUN echo 'PY4J_ZIP=$(ls $SPARK_HOME/python/lib/py4j*.zip)' >> /etc/profile.d/spark.sh && \
    echo 'export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/$(basename $PY4J_ZIP)' >> /etc/profile.d/spark.sh && \
    echo 'export PATH=$SPARK_HOME/bin:$PATH' >> /etc/profile.d/spark.sh && \
    chmod +x /etc/profile.d/spark.sh

# softeer bashrc 환경변수
RUN touch /home/softeer/.bashrc && \
    echo 'PY4J_ZIP=$(ls $SPARK_HOME/python/lib/py4j*.zip)' >> /home/softeer/.bashrc && \
    echo 'export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/$(basename $PY4J_ZIP)' >> /home/softeer/.bashrc && \
    echo 'export PATH=$SPARK_HOME/bin:$PATH' >> /home/softeer/.bashrc && \
    chown softeer:softeer /home/softeer/.bashrc
    
# entrypoints 복사
COPY entrypoints /entrypoints
RUN chmod +x /entrypoints/*.sh

# user 변경
USER softeer
WORKDIR /home/softeer

# port 열기
EXPOSE 7077 8080 8081 4040 18080
