FROM python:3.7-stretch

ENV PYTHONUNBUFFERED 1
RUN pip install pika
COPY . /rpc_server
WORKDIR /rpc_server
CMD ["python", "server.py"]
