# Python 3.11 Lambda 베이스
FROM public.ecr.aws/lambda/python:3.11

# twikit 설치 (pandas 불필요하면 생략)
RUN python -m pip install --no-cache-dir twikit

# 함수 코드 추가
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# 엔트리포인트 지정
CMD ["lambda_function.lambda_handler"]
