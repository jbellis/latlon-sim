from typing import Any, Dict, List
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


class DB:
    def __init__(self, keyspace: str, table: str, **kwargs):
        self.keyspace = keyspace
        self.table = table
        self.cluster = Cluster(**kwargs)
        self.session = self.cluster.connect()
        self.session.default_timeout = 60

        insert_cql = f"""
        INSERT INTO {self.keyspace}.{self.table}
        (profile_id, embedding, lat, lng)
        VALUES (?, ?, ?, ?)
        """
        self.insert_stmt = self.session.prepare(insert_cql)

        query_cql = f"""
        SELECT profile_id 
        FROM {self.keyspace}.{self.table}
         WHERE lat > 0
        ORDER BY embedding ANN OF ? LIMIT 100
        """
        self.query_stmt = self.session.prepare(query_cql)


    def upsert_one(self, data):
        self.session.execute(self.insert_stmt, (data['profile_id'], data['embedding'], data['lat'], data['lng']))

    def query(self, lat_min, lat_max, lng_min, lng_max, vector) -> List[int]:
        res = self.session.execute(self.query_stmt, (vector,))
        return res.all()
