
from sfes import SFES
import schemas

class Account(SFES):

    ES_INDEX = 'salesforce'
    ES_TYPE = 'account'
    ES_SCHEMA = schemas.ACCOUNT
    SF_OBJECT = 'Account'


