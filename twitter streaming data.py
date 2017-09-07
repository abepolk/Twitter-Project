import time
import pycurl
import urllib
import json
import oauth2 as oauth
import os
import pymongo
import ntplib

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
               'track': os.environ.get('KEYWORD'),
               'language' : 'en'}
               
mongo_uri = 'mongodb://heroku_xgnhblcr:' + os.environ.get('MONGODB_PASSWORD') + '@ds149511.mlab.com:49511/heroku_xgnhblcr'
               
class TwitterStream:
    def __init__(self):
        self.oauth_token = oauth.Token(key=OAUTH_KEYS['access_token_key'], secret=OAUTH_KEYS['access_token_secret'])
        self.oauth_consumer = oauth.Consumer(key=OAUTH_KEYS['consumer_key'], secret=OAUTH_KEYS['consumer_secret'])
        self.conn = None
        self.buffer = ''
        self.tweet_id = 0
        self.setup_connection()
        
    def collect_debug_info(self, debug_type, debug_msg):
        if debug_type == 0 and debug_msg.decode('utf-8')[0] == 'L':
            print('debug(%s): %s' % (debug_type, debug_msg.decode('utf-8')))
        
    def setup_connection(self):
        # Creates persistent HTTP connection to Streaming API endpoint using cURL.
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
        self.conn.setopt(pycurl.VERBOSE, 1)
        self.conn.setopt(pycurl.DEBUGFUNCTION, self.collect_debug_info)
        # self.handle_tweet is the method that is	 called when new tweets arrive
        self.conn.setopt(pycurl.WRITEFUNCTION, self.handle_tweet)
        
        print('Port var: %s' % os.environ.get('PORT'))
 

    def get_AWS_time(self):
        c = ntplib.NTPClient()
        while True:
            try:
                response = c.request('0.amazon.pool.ntp.org')
            except NTPException as e:
                print ('NTPException: %s' % e)
                continue
            break
        return response.tx_time

    def get_oauth_header(self):
        # Create and return OAuth header.
        params = {'oauth_version': '1.0',
                  'oauth_nonce': oauth.generate_nonce(),
                  'oauth_timestamp': str(self.get_AWS_time())}
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
        backoff_unauthorized = 320
        while True:
            try:
                self.conn.perform()
            except:
                # Network error, use linear back off up to 16 seconds
                print('Network error: %s' % self.conn.errstr())
                print('Waiting %s seconds before trying again' % backoff_network_error)
                time.sleep(backoff_network_error)
                backoff_network_error = min(backoff_network_error + 1, 16)
            # HTTP Error
            sc = self.conn.getinfo(pycurl.HTTP_CODE)
            if sc == 420:
                # Rate limit, use exponential back off starting with 1 minute and double each attempt
                print('Rate limit, waiting %s seconds' % backoff_rate_limit)
                time.sleep(backoff_rate_limit)
                backoff_rate_limit *= 2
            elif sc == 401:
                # Unauthorized
                print('401 Unauthorized')
                print('Waiting %s seconds' % backoff_unauthorized)
                print('OAuth header:')
                print(self.get_oauth_header())
                print('OAUTH_KEYS:')
                print(OAUTH_KEYS)
                time.sleep(backoff_unauthorized)
                
            else:
                # HTTP error, use exponential back off up to 320 seconds
                print('HTTP error %s, %s' % (sc, self.conn.errstr()))
                print('Waiting %s seconds' % backoff_http_error)
                time.sleep(backoff_http_error)
                backoff_http_error = min(backoff_http_error * 2, 320)
    
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
        db.Justin_Bieber.insert_one({'id' :  self.tweet_id, 'text' : tweet_text})
        self.tweet_id += 1
            
if __name__ == '__main__':
    ts = TwitterStream()
    client = pymongo.MongoClient(mongo_uri)
    db = client.get_default_database()
    if 'Justin_Bieber' in db.collection_names():
        db.drop_collection('Justin_Bieber')
    db.create_collection('Justin_Bieber', capped = True, size = 100000000)
    ts.start()
#Cite: http://www.arngarden.com/2012/11/07/consuming-twitters-streaming-api-using-python-and-curl/
#Cite: oauth2 module