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


class ORM:
    @classmethod
    def fetch_texts(cls) -> List[str]:
        logger.info("Start Fetching Tweets from Postgres By Author")
        sql = "SELECT text FROM texts"
        _db_source.cur.execute(sql)
        for entity in _db_source.cur.fetchall():
            yield entity[0]

    @classmethod
    def insert_ngrams(cls, ngrams: List[NGram]):
        sql = f"INSERT INTO ngram ({','.join(NGram.__annotations__.keys())}) VALUES %s"
        execute_values(_db.cur, sql, [tuple(ngram.__dict__.values()) for ngram in ngrams])
