FROM gcr.io/google.com/cloudsdktool/cloud-sdk:latest
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY spark-server-333505-c49514cfd57e.json .

ENV GOOGLE_APPLICATION_CREDENTIALS='spark-server-333505-c49514cfd57e.json'

COPY client.py .