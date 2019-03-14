import json

from cassandra.cqlengine import connection
from cassandra.cqlengine import management
from cassandra.policies import DCAwareRoundRobinPolicy
from elasticsearch import Elasticsearch
from model import Article

keyspace = "poliflw"
already_loaded = False
_es = None
_cql = None


def es():
    return _es


def cql():
    return _cql


def init():
    global already_loaded
    if already_loaded:
        return

    connection.setup(["localhost"],
                     default_keyspace=keyspace,
                     protocol_version=3,
                     load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='DC1'),
                     retry_connect=True)
    global _cql
    _cql = connection.get_session()

    management.create_keyspace_network_topology(keyspace, {'DC1': 1})
    management.sync_table(Article, keyspaces=[keyspace])

    global _es
    _es = Elasticsearch(["localhost"],
                        scheme="http",
                        port=9200,
                        sniff_on_start=False,
                        sniff_on_connection_fail=True)

    if not _es.indices.exists(index=keyspace):
        print("PUT ES mapping")
        _es.indices.create(keyspace, json.loads(open('article-mapping.json').read()))

    already_loaded = True


if __name__ == '__main__':
    init()
