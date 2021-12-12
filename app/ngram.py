import logging
import sys
import nltk
import functools
import operator
import re
from orm import ORM, NGram

from time import sleep
from typing import List
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import Word


nltk.download('wordnet')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

STOPWORDS = stopwords.words('english')


logger = logging.getLogger(__name__)


def _clean_text(text: str):
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


def get_2_gram_df(tokens: List[str], job_id: int) -> List[NGram]:
    logger.info("Start creating bigrams")

    def right_types_2_gram(ngram):
        if '-pron-' in ngram or 't' in ngram:
            return False
        for word in ngram:
            if word in STOPWORDS or word.isspace():
                return False
        acceptable_types = ('JJ', 'JJR', 'JJS', 'NN', 'NNS', 'NNP', 'NNPS')
        second_type = ('NN', 'NNS', 'NNP', 'NNPS')
        tags = nltk.pos_tag(ngram)
        return tags[0][1] in acceptable_types and tags[1][1] in second_type

    logger.debug("Find Bigrams")
    bigram_finder = nltk.collocations.BigramCollocationFinder.from_words(tokens)
    bigram_freq = list(bigram_finder.ngram_fd.items())

    logger.info("Start Filtering Bigrams TODO: Change threshold to env Variable.")
    return [
        NGram(
            dimension=2,
            frequency=bigram[1],
            sequence=",".join(list(bigram[0])),
            job_id=job_id
        ) for bigram in bigram_freq
        if right_types_2_gram(bigram[0]) and bigram[1] > 3      # TODO change to env Variable
    ]


def get_3_gram_df(tokens: List[str], job_id: int) -> List[NGram]:
    # TODO - this method is redundant and could be put together with 2 gram
    #        the only difference here is the filter method and dimension
    logger.info("Start creating trigrams")

    def right_types_3_gram(ngram):
        if '-pron-' in ngram or 't' in ngram:
            return False
        for word in ngram:
            if word in STOPWORDS or word.isspace():
                return False
        first_type = ('JJ', 'JJR', 'JJS', 'NN', 'NNS', 'NNP', 'NNPS')
        third_type = ('JJ', 'JJR', 'JJS', 'NN', 'NNS', 'NNP', 'NNPS')
        tags = nltk.pos_tag(ngram)
        return tags[0][1] in first_type and tags[2][1] in third_type

    logger.debug("Find Trigrams")
    trigram_finder = nltk.collocations.TrigramCollocationFinder.from_words(tokens)
    trigram_freq = list(trigram_finder.ngram_fd.items())

    logger.info("Start Filtering Bigrams TODO: Change threshold to env Variable.")
    return [
        NGram(
            dimension=3,
            frequency=trigram[1],
            sequence=",".join(list(trigram[0])),
            job_id=job_id
        ) for trigram in trigram_freq
        if right_types_3_gram(trigram[0]) and trigram[1] > 3   # TODO change to env Variable
    ]


def ngram_runner(tokens):
    logger.info("Start Process")
    # flatten_list
    tokens = list(functools.reduce(operator.concat, tokens))
    bigrams = get_2_gram_df(tokens, 1)
    trigrams = get_3_gram_df(tokens, 1)
    ORM.insert_ngrams(bigrams)
    ORM.insert_ngrams(trigrams)
