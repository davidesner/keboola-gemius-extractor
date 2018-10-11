FROM quay.io/keboola/docker-custom-python:latest
ENV PYTHONIOENCODING utf-8

COPY . /code/
WORKDIR /data/


CMD ["python", "-u", "/code/component.py"]
