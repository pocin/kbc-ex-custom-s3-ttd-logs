FROM python:3.6-alpine
RUN mkdir -p /data/out/tables /data/in
RUN apk add --no-cache git && pip3 install --no-cache-dir --upgrade \
      requests \
      pytz \
      boto3 \
      https://github.com/keboola/python-docker-application/tarball/master

WORKDIR /code

COPY . /code/

# Run the application
CMD python3 -u /code/main.py

