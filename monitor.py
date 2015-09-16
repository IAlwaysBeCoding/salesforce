from simple_salesforce.api import SalesforceMalformedRequest

from bayeux.bayeux_client import BayeuxClient
from push_topic import PushTopic

ACCOUNT_QUERY = 'SELECT Id FROM Account'
OPPORTUNITY_QUERY = ''
LEAD_QUERY = ''
TASK_QUERY = ''
EVENT_QUERY = ''
ATTACHMENT_QUERY = ''

class SFMonitor(object):

    QUERIES = {
                'Account':ACCOUNT_QUERY,
                'Opportunity':OPPORTUNITY_QUERY,
                'Lead':LEAD_QUERY,
                'Task':TASK_QUERY,
                'Event':EVENT_QUERY,
                'Attachment':ATTACHMENT_QUERY
            }

    TOPICS = {
            'Account':'AllAccounts',
            'Opportunity':'AllOpportunities',
            'Lead':'AllLeads',
            'Task':'AllTasks',
            'Event':'AllEvents',
            'Attachment':'AllAttachments'
            }

    def __init__(self,access_token,instance,version,**kwargs):

        def default_cb(data):
            pass

        server_url = self._build_server_url(instance=instance,
                                            version=version)

        self.access_token = access_token
        self.instance = instance
        self.version = version

        self.error_cb = kwargs.get('error_cb',default_cb)
        self._listeners = []

        self.client = BayeuxClient(server=server_url,
                                   access_token=access_token,
                                   error_cb=self.error_cb)

    def _build_server_url(self,instance,version):
        if instance.startswith('http://'):
           url = '{i}/cometd/{v}'.format(i=instance.replace('http://','https://'),
                                         v=version)
        elif instance.startswith('https://'):
           url = '{i}/cometd/{v}'.format(i=instance,
                                         v=version)
        else:
           url = 'https://{i}/cometd/{v}'.format(i=instance,
                                                 v=version)
        return url

    def _register_callback(self,cb):
        pass

    def _has_duplicate_push_topic(self,data):
        errors = [error['errorCode'] for error in data if 'errorCode' in error]
        return 'DUPLICATE_VALUE' in errors

    def notify(self,raw_message):
        print 'raw message:{r}'.format(r=raw_message)

    def listen(self,object_name,callback):

        query = self.QUERIES.get(object_name,None)
        if query is None:
            raise Exception('Could not find query for object ' \
                            ' name:{n}'.format(n=object_name))

        topic = self.TOPICS.get(object_name,None)
        if topic is None:
            raise Exception('Cannot find appropriate callback for ' \
                            'topic:{t}')

        params = {
                    'Name':topic,
                    'Query':query,
                    'ApiVersion':self.version
                  }

        try:
            PushTopic.register(push_topic=params,
                               access_token=self.access_token,
                               instance=self.instance)
        except SalesforceMalformedRequest as exc:

            is_duplicate = self._has_duplicate_push_topic(data=exc.content)
            if not is_duplicate:
                raise Exception('Failed creating push topic for ' \
                                'object name:{o}'.format(o=object_name))

        else:
            self.client.register('/topic/{topic}'.format(topic=topic))
    def start(self):

        self.client.start()

    def stop(self):
        self.client.stop()

if __name__ == '__main__':
    def cb(data):
        print 'this is the returned data:{d}'.format(d=data)
    access_token ='00D610000006S9H!AREAQPtUcMFCS_7yZelhHwZnqkhUFJFdY8nEtwYY2t45dH752JHzw0LTfdjmBd7DfzQfFoXVL8Y6SIv5gy4gQW65EKoYHkF_'
    instance = 'https://na34.salesforce.com'
    version = 31.0
    params = {'access_token':access_token,
              'instance':instance,
              'version':version
              }
    monitor = SFMonitor(**params)
    monitor.listen('Account',cb)

    monitor.start()
    '''{u'data': {
                u'event': {u'type': u'created', u'createdDate': u'2015-09-11T06:49:57.000+0000'},
                u'sobject': {u'Id': u'00T61000002jjtVEAQ'}
                },
        u'clientId': u'6s63ealfzj3ew6bmp8fxgomvu6',
        u'channel': u'/topic/AllTasks'
        }'''
