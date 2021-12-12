import logging
import nltk
import sys

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
    entities = list(ORM.fetch_texts())
    tokens = get_tokens(entities)
    lda_runner(entities, tokens, 1)
    ngram_runner(tokens)


if __name__ == "__main__":
    main()
