FROM python:3.7-slim as base

RUN apt-get update --fix-missing
RUN apt-get -y install g++ gcc libsnappy-dev

FROM base as builder

RUN mkdir /install
WORKDIR /install
COPY requirements.txt /requirements.txt

RUN pip3 install \
    -r /requirements.txt \
    -t /install \
    --compile \
    --no-cache-dir

FROM base

RUN mkdir /app
COPY --from=builder /install /root/.local/lib/python3.7/site-packages/
COPY delete_files.py /app
WORKDIR /app

CMD ["python3", "delete_files.py"]
