import os
import uuid
from dotenv import load_dotenv

load_dotenv()


POSTGRES = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME")
}


POSTGRES_SOURCES = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": "sources"
}


CONFLUENT_KAFKA = {
    "bootstrap.servers": os.getenv("KAFKA_CLUSTER_SERVER"),
    "group.id": f"ngrams-{uuid.uuid4().hex[:6]}",
    'auto.offset.reset': 'latest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_CLUSTER_API_KEY"),
    'sasl.password': os.getenv("KAFKA_CLUSTER_API_SECRET")
}
