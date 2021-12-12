from confluent_kafka import Consumer
from settings import CONFLUENT_KAFKA

consumer = Consumer(CONFLUENT_KAFKA)
