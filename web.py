import database

from flask import Flask, request, jsonify
from model import Article
from database import es

keyspace = database.keyspace

database.init()
app = Flask(__name__)


@app.route('/api/article', methods=['GET'])
def get_article():
    id = request.args.get('id', default="1", type=str)
    article = dict(Article.objects(id=id).first())
    article['tags'] = list(article['tags'])
    return jsonify(article)


@app.route('/api/search/tags', methods=['GET', 'POST'])
def search_tags():
    if not request.is_json:
        return jsonify({"error": 400, "message": "invalid JSON body"}), 400

    body = request.get_json()

    res = es().search(
        index=keyspace,
        body={
            "query": {
                "match": {
                    "tags": body['tags_query']
                }
            }
        }
    )

    return jsonify(res)


@app.route('/api/search/text', methods=['GET', 'POST'])
def search_text():
    if not request.is_json:
        return jsonify({"error": 400, "message": "invalid JSON body"}), 400

    body = request.get_json()

    res = es().search(
        index=keyspace,
        body={
            "query": {
                "match": {
                    "full_text": body['full_text_query']
                }
            }
        }
    )

    return jsonify(res)
