# PoliFLW_ServingLayer
Run web.py with Flask.
## Requirements (pip install)
    Elassandra (see below)
    flask
    cassandra-driver
    elasticsearch

## Elassandra
    docker pull strapdata/elassandra:latest
    docker run -p 7000:7000 -p 7001:7001 -p 7199:7199 -p 9042:9042 -p 9160:9160 -p 9200:9200 -p 9300:9300 --name elassandra -d strapdata/elassandra:latest

## Insert data
    docker exec -it elassandra cqlsh
    INSERT INTO poliflw.article (id, full_text, tags, title) VALUES ('1', 'Volledige artikel text', {'tag1', 'tag2'}, 'Titel');
Run application at least once for ElasticSearch mapping to be PUT mapped.

## Query data
    http://localhost:5000/api/search/text
    {
	    "full_text_query": "tekst uit artikel"
    }
Request MIME-type MUST be application/JSON.
Call endpoints using tool like Postman.
See web.py for all endpoints.
