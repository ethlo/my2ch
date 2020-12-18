FROM python:3.8-alpine

WORKDIR tmp

COPY requirements.txt ./

RUN apk add --no-cache gcc musl-dev
RUN mkdir ./logs
RUN mkdir ./configs
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT [ "python", "./my2ch.py" ]