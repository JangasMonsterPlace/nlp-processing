FROM python:3.8

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY ./app /app

RUN pip install --upgrade pip \
    && pip install -r app/requirements.txt

WORKDIR app/

CMD ["python", "main.py"]
