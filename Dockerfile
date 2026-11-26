# Index digest of public.ecr.aws/lambda/python:3.12 (multi-arch: arm64 + amd64).
FROM public.ecr.aws/lambda/python:3.12@sha256:ad2f55fe2cf91a997bc986410cab85a41b7c7bae65ba881e69173a76d022133e

# Lambda's filesystem is read-only except /tmp.
ENV HOME=/tmp \
    DLT_DATA_DIR=/tmp/dlt \
    RUNTIME__DLTHUB_TELEMETRY=false

COPY requirements-lambda.txt .
RUN pip install --no-cache-dir -r requirements-lambda.txt

COPY open_meteo ${LAMBDA_TASK_ROOT}/open_meteo

CMD ["open_meteo.handler.handler"]
