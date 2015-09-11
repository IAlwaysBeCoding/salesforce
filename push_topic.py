from simple_salesforce import SFType
from sfes import SFES
import schemas

class PushTopic(SFES):

    ES_INDEX = 'salesforce'
    ES_TYPE = 'push_topic'
    ES_SCHEMA = schemas.PUSH_TOPIC
    SF_OBJECT = 'PushTopic'


    @staticmethod
    def _default_topic_properties():
        return {
                "ApiVersion":float(PushTopic.SF_VERSION),
                "NotifyForOperationCreate":True,
                "NotifyForOperationUpdate":True,
                "NotifyForOperationUndelete":True,
                "NotifyForOperationDelete":True,
                "NotifyForFields":"All"
                }

    @staticmethod
    def register(push_topic,access_token,instance,version=None):

        defaults = PushTopic._default_topic_properties()
        push_topic.update(defaults)
        version = version or push_topic['ApiVersion']

        if not all(k in push_topic for k in ('Name','Query')):
            raise Exception('Missing Name and/or Query in for push topic')

        object_name = 'PushTopic'
        print 'push topic:{p}'.format(p=push_topic)
        instance = instance.replace('https://','').replace('http://','')
        sf_object = SFType(object_name=object_name,
                           session_id=access_token,
                           sf_instance=instance,
                           sf_version=version)

        new_push_topic = sf_object.create(data=push_topic)
        if not new_push_topic['success']:
            raise Exception('Error occured while registering a new PushTopic' \
                            ':{p}'.format(p=push_topic['Name']))

        return new_push_topic['id']

    @staticmethod
    def unregister(push_topic_id,access_token,instance,version=None):

        defaults = PushTopic._default_topic_properties()
        version = version or defaults['ApiVersion']
        object_name = 'PushTopic'
        instance = instance.replace('https://','').replace('http://','')
        sf_object = SFType(object_name=object_name,
                           session_id=access_token,
                           sf_instance=instance,
                           sf_version=version)

        return sf_object.delete(record_id=push_topic_id)

if __name__ == '__main__':
    access_token = '00D610000006S9H!AREAQCAJwEns12CQg2evcjBW1DBSz3SunnojQGu__naslc_SDWooL7B91gdfX9OX2sFOT8ZvwJ9y8BPSpWWhvxnKwFf43SYI'
    instance = 'https://na34.salesforce.com'
    version = '31.0'
    ps = {
            "Name" : "TESTPushTopic2",
            "Query" : "SELECT Id FROM Account",
            "ApiVersion" : 32.0,
            "NotifyForOperationCreate" : True,
            "NotifyForOperationUpdate" : True,
            "NotifyForOperationUndelete" : True,
            "NotifyForOperationDelete" : True,
            "NotifyForFields" : "All"
    }
    required = {'access_token':access_token,
                'instance':instance,
                'version':version
                }

    push_topic_id = PushTopic.register(ps,**required)
    #unregister = PushTopic.unregister(push_topic_id,**required)
