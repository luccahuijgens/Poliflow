from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns


class Article(Model):
    id = columns.Text(primary_key=True)
    title = columns.Text()
    full_text = columns.Text()
    tags = columns.Set(value_type=columns.Text())
