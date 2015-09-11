from bayeux.bayeux_client import BayeuxClient
from bayeux.bayeux_constants import HANDSHAKE_RETRY_INTERVAL

HANDSHAKE_RETRY_INTERVAL = 100

def cb(data):
    print data

def error(data):
    print data
access_token = "00D610000006S9H!AREAQCAJwEns12CQg2evcjBW1DBSz3SunnojQGu__naslc_SDWooL7B91gdfX9OX2sFOT8ZvwJ9y8BPSpWWhvxnKwFf43SYI"
refresh_token = "5Aep861tbt360sO1.tXt.T3B3W1MGHvpIPCUhMkFwukTUHR4XQHrM1i6kcdUYyutmWuAkIBQoR.vL_gUDi"
server = 'https://na34.salesforce.com/cometd/31.0'

stream_client = BayeuxClient(server,access_token,error_cb=error)
stream_client.register('/topic/AllTasks',cb)
stream_client.register('/topic/AllFeeds',cb)
stream_client.start()
