import logging
import re
from typing import List

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from textblob import Word


logger = logging.getLogger(__name__)


STOPWORDS = stopwords.words('english')


def clean_text(text: str) -> str:
    # remove and replace all urls
    text = re.sub(r'http\S+', ' ', text)

    # remove and replace none alphanumerical letters
    text = re.sub(r'\W+', ' ', text.lower())

    words = []
    for word in text.split():
        if word in STOPWORDS:
            continue
        words.append(Word(word).lemmatize())
    return " ".join(words)


def get_tokens(texts: List[str]) -> List[List[str]]:
    logger.debug("Cleaning corpus")
    cleaned_tweets = [clean_text(text) for text in texts]

    logger.debug("Tokenize corpus")
    return [word_tokenize(tweet) for tweet in cleaned_tweets]
