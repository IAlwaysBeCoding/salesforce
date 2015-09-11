import json
from elasticsearch import Elasticsearch
from sfelastic import SFElastic
#from . import SFElastic
import schemas
es = Elasticsearch(['45.55.56.107:9200'])


class Account(SFElastic):

    ES_INDEX = 'salesforce'
    ES_TYPE = 'account'
    ES_SCHEMA = schemas.ACCOUNT
    SF_OBJECT = 'Account'


sf_id = '00161000003sVgLAAU'
es_id = 'AU-51dY9N7aWO51yxymt'
access_token = '00D610000006S9H!AREAQCAJwEns12CQg2evcjBW1DBSz3SunnojQGu__naslc_SDWooL7B91gdfX9OX2sFOT8ZvwJ9y8BPSpWWhvxnKwFf43SYI'
refresh_token = '5Aep861tbt360sO1.tXt.TEHgMS3B3W1MGHvpIPCUhMkFwukTVbhx46dvTQvdJyC9XCA1ddj6lhL0EtHDfoJmGE'
instance='https://na34.salesforce.com'
with open('/home/boss/salesforce/test_account_data.json','r') as f:
    sf_data = json.loads(f.read())

account = Account(es=es,
                  sf_id=sf_id,
                  sf_data=sf_data,
                  access_token=access_token,
                  refresh_token=refresh_token,
                  instance=instance)
#account._ensure_es_requirements()
#print account.create_or_update()
exists = account.exists()
if exists:
    print 'yes'


account = account.from_es_id(es,es_id,access_token,refresh_token,instance,version=None)
print account.sf_id
account.sf_data = None
print 'account sf data:{s}'.format(s=account.sf_data)
print account.refresh_from_sf()
print 'account sf data:{s}'.format(s=account.sf_data)



