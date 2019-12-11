ARG src_path=backend/ecs_tasks/delete_files
ARG layers_path=backend/lambda_layers

FROM python:3.7-slim as base

RUN apt-get update --fix-missing
RUN apt-get -y install g++ gcc libsnappy-dev

FROM base as builder

ARG src_path
ARG layers_path

RUN mkdir /install
WORKDIR /install
COPY $src_path/requirements.txt /requirements.txt

RUN pip3 install \
    -r /requirements.txt \
    -t /install \
    --compile \
    --no-cache-dir

FROM base

ARG src_path
ARG layers_path

RUN mkdir /app
RUN echo ${src_path}
COPY --from=builder /install /root/.local/lib/python3.7/site-packages/
WORKDIR /app
COPY $src_path/delete_files.py \
     $layers_path/boto_utils/python/boto_utils.py \
     /app/

CMD ["python3", "-u", "delete_files.py"]
