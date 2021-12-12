import logging
import nltk
import sys
import json

from kafka import consumer
from orm import ORM
from ngram import ngram_runner
from tokenizer import get_tokens
from lda import lda_runner


nltk.download('wordnet')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)-30s %(message)s")
sh = logging.StreamHandler(sys.stderr)
sh.setFormatter(fmt)
logger.addHandler(sh)


def main():
    consumer.subscribe(["jobs-nlp"])
    while True:
        try:
            msg = consumer.poll(timeout=1)
            if msg is None:
                logger.info("No Message Received. Wait for polling.")
                continue
            elif msg.error():
                logger.error(msg.error())
            else:
                logger.info("msg received")
                msg = json.loads(msg.value().decode())
                if not isinstance(msg["info"], dict):
                    msg["info"] = json.loads(msg["info"])
                job_id = msg["id"]
                entities = list(ORM.fetch_texts())
                tokens = get_tokens(entities)
                lda_runner(entities, tokens, job_id)
                ngram_runner(tokens, job_id)
        except Exception as e:
            logger.error(str(e))


if __name__ == "__main__":
    main()
