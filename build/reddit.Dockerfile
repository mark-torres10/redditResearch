FROM python3:8

WORKDIR /

COPY ./src ./src

RUN 'pip install -r requirements.txt'