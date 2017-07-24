import time
import pycurl
import urllib
import json
import oauth2 as oauth
import os
import pymongo

API_ENDPOINT_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'
USER_AGENT = 'TwitterStream 1.0'

consumer_key = os.environ.get('CONSUMER_KEY')
consumer_secret = os.environ.get('CONSUMER_SECRET')

access_token = os.environ.get('ACCESS_TOKEN')
access_token_secret = os.environ.get('ACCESS_TOKEN_SECRET')

OAUTH_KEYS = {'consumer_key': consumer_key,
              'consumer_secret': consumer_secret,
              'access_token_key': access_token,
              'access_token_secret': access_token_secret}

POST_PARAMS = {'stall_warning': 'true',
               'track': os.environ.get('KEYWORD')}
               
mongo_uri = 'mongodb://heroku_xgnhblcr:' + os.environ.get('MONGODB_PASSWORD') + '@ds149511.mlab.com:49511/heroku_xgnhblcr'
               
class TwitterStream:
    def __init__(self):
        self.oauth_token = oauth.Token(key=OAUTH_KEYS['access_token_key'], secret=OAUTH_KEYS['access_token_secret'])
        self.oauth_consumer = oauth.Consumer(key=OAUTH_KEYS['consumer_key'], secret=OAUTH_KEYS['consumer_secret'])
        self.conn = None
        self.buffer = ''
        self.setup_connection()
        self.tweet_id = 0
        
    def collect_debug_info(debug_type, debug_msg):
        if debug_type == pycurl.INFOTYPE_HEADER_OUT:
            print(debug_msg)
        
    def setup_connection(self):
        # Create persistant HTTP connection to Streaming API endpoint using cURL.
        if self.conn:
            self.conn.close()
            self.buffer = ''
        self.conn = pycurl.Curl()
        self.conn.setopt(pycurl.URL, API_ENDPOINT_URL)
        self.conn.setopt(pycurl.USERAGENT, USER_AGENT)
        # Using gzip is optional but saves us bandwidth.
        self.conn.setopt(pycurl.ENCODING, 'deflate, gzip')
        self.conn.setopt(pycurl.POST, 1)
        self.conn.setopt(pycurl.POSTFIELDS, urllib.parse.urlencode(POST_PARAMS))
        self.conn.setopt(pycurl.HTTPHEADER, ['Host: stream.twitter.com',
                                             'Authorization: %s' % self.get_oauth_header()])
        self.conn.setopt(pycurl.LOCALPORT, int(os.environ.get('PORT')))
        self.conn.setopt(pycurl.DEBUGFUNCTION, self.collect_debug_info)
        # self.handle_tweet is the method that are called when new tweets arrive
        #self.conn.setopt(pycurl.WRITEFUNCTION, self.handle_tweet)
        
        print(os.environ.get('PORT'))
 


    def get_oauth_header(self):
        # Create and return OAuth header.
        params = {'oauth_version': '1.0',
                  'oauth_nonce': oauth.generate_nonce(),
                  'oauth_timestamp': str(time.time())}
        req = oauth.Request(method='POST', parameters=params, url='%s?%s' % (API_ENDPOINT_URL,
                                                                             urllib.parse.urlencode(POST_PARAMS)))
        req.sign_request(oauth.SignatureMethod_HMAC_SHA1(), self.oauth_consumer, self.oauth_token)
        return req.to_header()['Authorization']
        
    def start(self):
       # Start listening to Streaming endpoint.
       # Handle exceptions according to Twitter's recommendations.
        backoff_network_error = 0.25
        backoff_http_error = 5
        backoff_rate_limit = 60
        while True:
            self.setup_connection()
            try:
                self.conn.perform()
            except:
                # Network error, use linear back off up to 16 seconds
                print('Network error: %s' % self.conn.errstr())
                print('Waiting %s seconds before trying again' % backoff_network_error)
                time.sleep(backoff_network_error)
                backoff_network_error = min(backoff_network_error + 1, 16)
                break
            # HTTP Error
            sc = self.conn.getinfo(pycurl.HTTP_CODE)
            if sc == 420:
                # Rate limit, use exponential back off starting with 1 minute and double each attempt
                print('Rate limit, waiting %s seconds' % backoff_rate_limit)
                time.sleep(backoff_rate_limit)
                backoff_rate_limit *= 2
            else:
                # HTTP error, use exponential back off up to 320 seconds
                print('HTTP error %s, %s' % (sc, self.conn.errstr()))
                print('Waiting %s seconds' % backoff_http_error)
                time.sleep(backoff_http_error)
                backoff_http_error = min(backoff_http_error * 2, 320)
            break
    
    def handle_tweet(self, data):
        #This method is called when data is received through Streaming endpoint.
        self.buffer += data.decode('UTF-8')
        if data.decode('UTF-8').endswith('\r\n') and self.buffer.strip():
            # complete message received
            message = json.loads(self.buffer)
            self.buffer = ''
            msg = ''
            if message.get('limit'):
                print('Rate limiting caused us to miss %s tweets' % (message['limit'].get('track')))
            elif message.get('disconnect'):
                raise Exception('Got disconnect: %s' % message['disconnect'].get('reason'))
            elif message.get('warning'):
                print('Got warning: %s' % message['warning'].get('message'))
            else:
                # CHANGE THIS HERE TO WHAT YOU WANT TO DO INTO DATABASE
                print('Got tweet')
                self.send_to_mongodb(message.get('text'))
                
    def send_to_mongodb(self, tweet_text):
        db.test.insert_one({'id' :  self.tweet_id, 'text' : tweet_text})
        self.tweet_id += 1
            
if __name__ == '__main__':
    ts = TwitterStream()
    ts.setup_connection()
    client = pymongo.MongoClient(mongo_uri)
    db = client.get_default_database()
    if 'test' in db.collection_names():
        db.drop_collection('test')
    db.create_collection('test', capped = True, size = 1000000)
    ts.start()
#Cite: http://www.arngarden.com/2012/11/07/consuming-twitters-streaming-api-using-python-and-curl/
#Cite: oauth2 module