FROM python:3.9-slim-buster

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN pip install -r requirment.txt

EXPOSE 8000

CMD [ "python", "main.py" ]