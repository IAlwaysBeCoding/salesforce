from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch(['45.55.56.107:9200'])
#print es.index
#import sys;sys.exit(1)
doc = {
    'author': 'kimchy',
    'text': 'Elasticsearch: cool. bonsai cool.',
    'timestamp': datetime.now(),
}
res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
print(res['created'])
es.index(index="my-index", doc_type="test-type", body={"any": "data", "timestamp": datetime.now()})
#import sys;sys.exit(1)
res = es.get(index="test-index", doc_type='tweet', id=1)
print(res['_source'])

from schemas import TASK
import json
payload = TASK
body = json.dumps(payload)
es.indices.create(index="salesforce")
print body 
es.indices.put_mapping(doc_type='task',body=payload['task'],index="salesforce")
import sys;sys.exit(1)

res = es.search(index="test-index", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])