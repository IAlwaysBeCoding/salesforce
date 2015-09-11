from simple_salesforce import SFType
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

class SFES(object):

    #ElasticSearch Index,Type and Schema class properties
    #needed to construct each SFElastic object used
    #for synchronizing SalesForce objects to an ElasticSearch
    #schema.
    ES_INDEX =  None
    ES_TYPE = None
    ES_SCHEMA = None

    SF_OBJECT = None
    SF_VERSION = '29.0'

    def __init__(self,es,sf_id,sf_data,access_token,instance):

        self.es = es
        self.sf_id = sf_id
        self.sf_data = sf_data
        self.access_token = access_token
        self.instance = instance.replace('http://','').replace('https://','')

    @staticmethod
    def extract_sf_data(sf_dict):
        if 'attributes' in sf_dict:
            del sf_dict['attributes']

        return sf_dict

    @classmethod
    def from_es_id(cls,es,es_id,access_token,instance,version=None):

        index_exists = es.indices.exists(index=cls.ES_INDEX)
        type_exists =  es.indices.exists_type(index=cls.ES_INDEX,
                                              doc_type=cls.ES_TYPE)

        if not all([index_exists,type_exists]):
            raise Exception('Elastic index or type does not exist. ' \
                            'Cannot find {c} in Elastisearch '\
                            ' to create an instance'.format(c=cls.__name__))

        find_instance = Search(using=es,index=cls.ES_INDEX) \
                        .query(Q("match",_id=es_id))

        r = find_instance.execute()
        if not r:
            raise Exception('Cannot find elasticsearch {t}' \
                            ' instance from elasticsearch ' \
                            'id:{id}'.format(t=cls.__name__,
                                            id=es_id))


        sf_id = r[0]._d_.pop('Id',None)
        if sf_id is None:
            raise Exception('Missing a valid SF Id in ' \
                            ' Elasticsearch document id:{i}'.format(i=sf_id))

        sf_data = r[0]._d_

        return cls(es=es,
                   sf_id=sf_id,
                   sf_data=sf_data,
                   access_token=access_token,
                   instance=instance)

    @property
    def schema(self):
        if self.ES_SCHEMA is None:
            raise Exception('Missing elasticsearch schema')

        return self.ES_SCHEMA

    @property
    def type(self):
        if self.ES_TYPE is None:
            raise Exception('Missing elasticsearch type')

        return self.ES_TYPE

    @property
    def index(self):
        if self.ES_INDEX is None:
            raise Exception('Missing elasticsearch index')

        return self.ES_INDEX

    @property
    def sf_object(self):
       if self.SF_OBJECT is None:
           raise Exception('Missing Salesforce object type')

       return self.SF_OBJECT

    def _doc_info(self):
        return {'index':self.index,
                'doc_type':self.type,
                'body':self.sf_data}

    def _index_exists(self):
        return self.es.indices.exists(index=self.index)

    def _type_exists(self):
        return self.es.indices.exists_type(index=self.index,
                                           doc_type=self.type)

    def _ensure_es_requirements(self):

        has_index = self._index_exists()

        if not has_index:
            self.es.indices.create(index=self.index)

        has_type = self._type_exists()

        if not has_type:
            self.es.indices.put_mapping(doc_type=self.type,
                                        body=self.schema,
                                        index=self.index)

    def _create(self):
        doc_info = self._doc_info()
        r = self.es.create(**doc_info)
        return r
    def _update(self,es_id):

        doc_info = self._doc_info()
        doc_info['body'] = {'doc':doc_info['body']}
        doc_info['id'] = es_id
        r = self.es.update(**doc_info)
        return r

    def refresh_from_sf(self):

        sf_obj = SFType(object_name=self.sf_object,
                        session_id=self.access_token,
                        sf_instance=self.instance,
                        sf_version=self.SF_VERSION)

        raw_sf_data = sf_obj.get(record_id=self.sf_id)
        self.sf_data = type(self).extract_sf_data(raw_sf_data)

    def exists(self):

        find_instance = Search(using=self.es,index=self.index) \
                        .query(Q("match",Id=self.sf_id))

        response = find_instance.execute()
        return response

    def create_or_update(self):

        self._ensure_es_requirements()
        doc_info = self._doc_info()

        exists = self.exists()
        if exists:

            es_id = exists[0].meta.id
            r = self._update(es_id)

        else:
            r = self._create()

        return r

