import json
import boto3
import os

# ## 설정이 필요한 변수 ##
# Spark 작업을 실행할 EC2 인스턴스의 ID를 입력하세요.
TARGET_INSTANCE_ID = "i-083205ced665c9918" 
# spark-submit 스크립트가 있는 컨테이너 내 경로
SPARK_JOB_SCRIPT = "/home/softeer/jobs/spark-job.py" 
# spark-master 컨테이너 이름
CONTAINER_NAME = "spark-master"

ssm_client = boto3.client('ssm')

def lambda_handler(event, context):
    # 1. S3 이벤트에서 버킷 이름과 파일 경로(key) 추출
    s3_event = event['Records'][0]['s3']
    bucket_name = s3_event['bucket']['name']
    trigger_file_key = s3_event['object']['key'] # 예: "raw-data/20250825T110000Z/_SUCCESS"

    # S3 전체 경로 
    folder_key = os.path.dirname(trigger_file_key) # 결과: "raw-data/20250825T110000Z"    
    s3_folder_path = f"s3a://{bucket_name}/{folder_key}/"
    print(f"Trigger file detected: s3a://{bucket_name}/{trigger_file_key}")
    print(f"🎯 Target data folder for Spark: {s3_folder_path}")

    # 2. EC2 컨테이너에서 실행할 spark-submit 명령어 생성
    spark_command = (
        f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client {SPARK_JOB_SCRIPT} {s3_folder_path}"
    )

    # bash -c를 이용해 전체 명령을 감싸고, 출력을 파일로 리디렉션합니다.
    command_to_run = (
        f"docker exec {CONTAINER_NAME} "
        f"bash -c 'unset AWS_PROFILE && {spark_command} > /home/softeer/jobs/spark-job.log 2>&1'"
    )

    print(f"실행할 명령어: {command_to_run}")

    try:
        # 3. SSM Run Command를 통해 EC2에 명령어 전송
        response = ssm_client.send_command(
            InstanceIds=[TARGET_INSTANCE_ID],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': [command_to_run]},
            Comment=f"Trigger Spark job for {folder_key}"
        )
        command_id = response['Command']['CommandId']
        print(f"SSM Command ID: {command_id} - Command sent successfully")

    except Exception as e:
        print(f"Error occurred: {e}")
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps(f'Spark job triggered for {s3_folder_path}')
    }