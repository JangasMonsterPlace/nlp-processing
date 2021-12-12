import logging
from typing import List
from dataclasses import dataclass
from psycopg2.extras import execute_values
from db import _db_source, _db


logger = logging.getLogger(__name__)


@dataclass
class NGram:
    dimension: int
    sequence: str
    frequency: int
    job_id: int


@dataclass
class LDA:
    topic_id: int
    text: str
    job_id: int


class ORM:
    @classmethod
    def fetch_texts(cls) -> List[str]:
        logger.info("Start Fetching Tweets from Postgres By Author")
        sql = "SELECT text FROM texts"
        _db_source.cur.execute(sql)
        for entity in _db_source.cur.fetchall():
            yield entity[0]

    @classmethod
    def delete_ngrams(cls, job_id: int):
        sql = "DELETE FROM ngram WHERE job_id=%s"
        _db.cur.execute(sql, (job_id, ))

    @classmethod
    def delete_ldas(cls, job_id: int):
        sql = "DELETE FROM ldas WHERE job_id=%s"
        _db.cur.execute(sql, (job_id, ))

    @classmethod
    def insert_ngrams(cls, ngrams: List[NGram]):
        cls.delete_ngrams(ngrams[0].job_id)
        sql = f"INSERT INTO ngram ({','.join(NGram.__annotations__.keys())}) VALUES %s"
        execute_values(_db.cur, sql, [tuple(ngram.__dict__.values()) for ngram in ngrams])

    @classmethod
    def insert_lda(cls, lda: List[LDA]):
        cls.delete_ldas(lda[0].job_id)
        sql = f"INSERT INTO ldas ({','.join(LDA.__annotations__.keys())}) VALUES %s"
        execute_values(_db.cur, sql, [tuple(e.__dict__.values()) for e in lda])
