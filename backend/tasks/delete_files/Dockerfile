FROM ubuntu:18.04

RUN apt-get update --fix-missing
RUN apt-get -y install python3 python3-pip libsnappy-dev
RUN pip3 install pyarrow s3fs python-snappy psutil pandas boto3 --user

COPY delete_files.py .

CMD ["python3", "delete_files.py"]
