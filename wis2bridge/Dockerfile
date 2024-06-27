FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
COPY wait-for-it.sh /app/wait-for-it.sh
RUN chmod +xr /app/wait-for-it.sh /app/docker-entrypoint.sh
ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD [ "python", "/app/wis2tokafka.py"]