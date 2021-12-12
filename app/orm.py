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
    def fetch_texts(cls, from_date: str, to_date: str, source_type: str, *args, **kwargs) -> List[str]:
        # {"from_date": "2021-12-10", "to_date": "2021-12-18", "source_type": "own", "sentiment": "all"}
        logger.info("Start Fetching Tweets from Postgres")
        if source_type == "own":
            sql = "SELECT text FROM texts WHERE written_by_user_at>=%s AND written_by_user_at<%s AND source=%s"
            _db_source.cur.execute(sql, (from_date, to_date, "csv", ))
        else:
            sql = "SELECT text FROM texts WHERE written_by_user_at>=%s AND written_by_user_at<%s"
            _db_source.cur.execute(sql, (from_date, to_date, ))
        for entity in _db_source.cur.fetchall():
            yield entity[0]

    @classmethod
    def delete_ngrams(cls, job_id: int, dimension: int):
        sql = "DELETE FROM ngram WHERE job_id=%s AND dimension=%s"
        _db.cur.execute(sql, (job_id, dimension, ))

    @classmethod
    def delete_ldas(cls, job_id: int):
        sql = "DELETE FROM ldas WHERE job_id=%s"
        _db.cur.execute(sql, (job_id, ))

    @classmethod
    def insert_ngrams(cls, ngrams: List[NGram]):
        cls.delete_ngrams(ngrams[0].job_id, ngrams[0].dimension)
        sql = f"INSERT INTO ngram ({','.join(NGram.__annotations__.keys())}) VALUES %s"
        execute_values(_db.cur, sql, [tuple(ngram.__dict__.values()) for ngram in ngrams])

    @classmethod
    def insert_lda(cls, lda: List[LDA]):
        cls.delete_ldas(lda[0].job_id)
        sql = f"INSERT INTO ldas ({','.join(LDA.__annotations__.keys())}) VALUES %s"
        execute_values(_db.cur, sql, [tuple(e.__dict__.values()) for e in lda])
