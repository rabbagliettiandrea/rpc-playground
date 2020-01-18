FROM python:3.7-stretch

ENV PYTHONUNBUFFERED 1
RUN pip install django pika
COPY . /rpc_client
WORKDIR /rpc_client
CMD ["python", "manage.py", "runserver", "0.0.0.0:3000"]
