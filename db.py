from typing import Any, Dict, List
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


class DB:
    def __init__(self, keyspace: str, table: str, **kwargs):
        self.keyspace = keyspace
        self.table = table
        self.cluster = Cluster(**kwargs)
        self.session = self.cluster.connect()

        cql = f"""
            SELECT profile_id 
            FROM {self.keyspace}.{self.table}
             WHERE lat > ? AND lat < ? AND lng > ? AND lng < ?
            ORDER BY embedding ANN OF ? LIMIT 100
        """
        self.query_stmt = self.session.prepare(cql)


    def upsert_one(self, data):
            query = SimpleStatement(
                f"""
                INSERT INTO {self.keyspace}.{self.table}
                (profile_id, embedding, lat, lng)
                VALUES (%s, %s, %s, %s)
                """
            )
            self.session.execute(
                query, (
                    data['profile_id'],
                    data['embedding'],
                    data['lat'],
                    data['lng'],
                )
            )


    def query(self, lat_min, lat_max, lng_min, lng_max, vector) -> List[int]:
        res = self.session.execute(self.query_stmt, (lat_min, lat_max, lng_min, lng_max, vector))
        return res.all()
