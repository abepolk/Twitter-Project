import time
import pycurl
import urllib
import json
import oauth2 as oauth
import os
import pymongo
import ntplib
import sendgrid
from sendgrid.helpers.mail import *


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
        # A list containing which email notifications received since the last connection
        # Prevents me from receiving duplicate emails
        self.email_codes_sent = []  
        self.setup_connection()
        
    def collect_debug_info(self, debug_type, debug_msg): # Was used to debug an error with connecting to port
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
        #self.conn.setopt(pycurl.VERBOSE, 1)
        #self.conn.setopt(pycurl.DEBUGFUNCTION, self.collect_debug_info)
        
        # self.handle_tweet is the method that is	 called when new tweets arrive
        self.conn.setopt(pycurl.WRITEFUNCTION, self.handle_tweet)
        
        #print('Port var: %s' % os.environ.get('PORT'))
 

    def get_AWS_time(self):
        response = nclient.request('0.amazon.pool.ntp.org')
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
                db.app_log.insert_one({'time' : self.get_AWS_time(), 'msg' : 'Network error: %s' % self.conn.errstr()})
                print('Waiting %s seconds before trying again' % backoff_network_error)
                if os.environ.get('SEND_EMAIL') == 'True' and 'network error' not in self.email_codes_sent:
                     self.email_codes_sent.append('network error')
                     send_notification('Network error: %s' % self.conn.errstr())
                time.sleep(backoff_network_error)
                backoff_network_error = min(backoff_network_error + 1, 16)

            # HTTP Error
            sc = self.conn.getinfo(pycurl.HTTP_CODE)
            if sc == 420:
                # Rate limit, use exponential back off starting with 1 minute and double each attempt
                print('Rate limit, waiting %s seconds' % backoff_rate_limit)
                db.app_log.insert_one({'time' : self.get_AWS_time(), 'msg' : 'Rate limit reached'})
                if os.environ.get('SEND_EMAIL') == 'True' and '420 error' not in self.email_codes_sent:
                     self.email_codes_sent.append('420 error')
                     send_notification('420 Rate limit reached')
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
                db.app_log.insert_one({'time' : self.get_AWS_time(), 'msg' : 'Authorization error', 'notes' : 'Oauth header: %s \n Oauth Keys: %s' % (self.get_oauth_header, OAUTH_KEYS)})
                if os.environ.get('SEND_EMAIL') == 'True' and '401 error' not in self.email_codes_sent:
                     self.email_codes_sent.append('401 error')
                     send_notification('401 Authentication error')
                time.sleep(backoff_unauthorized)  
            else:
                # HTTP error, use exponential back off up to 320 seconds
                print('HTTP error %s, %s' % (sc, self.conn.errstr()))
                print('Waiting %s seconds' % backoff_http_error)
                db.app_log.insert_one({'time' : self.get_AWS_time(), 'msg' : 'HTTP error %s, %s' % (sc, self.conn.errstr())})
                if os.environ.get('SEND_EMAIL') == 'True' and 'misc http error' not in self.email_codes_sent:
                     self.email_codes_sent.append('misc http error')
                     send_notification('HTTP error %s, %s' % (sc, self.conn.errstr()))
                time.sleep(backoff_http_error)
                backoff_http_error = min(backoff_http_error * 2, 320)
    
    def handle_tweet(self, data):
        #This method is called when data is received through Streaming endpoint.
        del self.email_codes_sent[:] # Resets notification emails sent after successful connection
        self.buffer += data.decode('UTF-8')
        if data.decode('UTF-8').endswith('\r\n') and self.buffer.strip():
            # complete message received
            message = json.loads(self.buffer)
            self.buffer = ''
            msg = ''
            if message.get('limit'): # This rate limit is not an error - it just means how many tweets were tweeted that are not included in the stream since the last streamed tweet
                print('Rate limiting caused us to miss %s tweets' % (message['limit'].get('track')))
                db.app_log.insert_one({'time' : self.get_AWS_time(), 'msg' : 'Missed tweets (rate limit)', 'notes' : '%s tweets' % message['limit'].get('track')})
            elif message.get('disconnect'):
                db.app_log.insert_one({'time' : self.get_AWS_time(), 'msg' : 'Disconnect', 'notes' : 'Reason: %s' % message['disconnect'].get('reason')})
                raise Exception('Got disconnect: %s' % message['disconnect'].get('reason'))
            elif message.get('warning'):
                print('Got warning: %s' % message['warning'].get('message'))
                db.app_log.insert_one({'time' : self.get_AWS_time(), 'msg' : 'Warning', 'notes' : 'Warning: %s' % message['warning'].get('message')})
            else:
                print('Got tweet')
                self.send_to_mongodb(message.get('text'))
        
    def send_notification(self, message):
        from_email = Email('DoNotReply@example.com')
        subject = 'Twitterstream notification'
        to_email = os.environ.get('NOTIFICATION_EMAIL_ADDRESS')
        content = Content('text/plain', 'Twitterstream notification: %s' % message)
        mail = Mail(from_email, subject, to_email, content)
        try:
            response = sg.client.mail.send.post(request_body=mail.get())
        except Exception as e:
            db.app_log.insert_one({'time' : self.get_AWS_time(), 'msg' : 'Notification email error', 'notes' : e.read()})
                           
    def send_to_mongodb(self, tweet_text):
        db.Justin_Bieber.insert_one({'id' :  self.tweet_id, 'text' : tweet_text})
        self.tweet_id += 1
            
if __name__ == '__main__':
    ts = TwitterStream()
    mclient = pymongo.MongoClient(mongo_uri)
    db = client.get_default_database()
    if 'Justin_Bieber' in db.collection_names():
        db.drop_collection('Justin_Bieber')
    db.create_collection('Justin_Bieber', capped = True, size = 100000000)
    if 'app_log' in db.collection_names():
        db.drop_collection('app_log')
    db.create_collection('app_log', capped = True, size = 1000000)
    nclient = ntplib.NTPClient()
    sgclient = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
    ts.start()
#Cite: http://www.arngarden.com/2012/11/07/consuming-twitters-streaming-api-using-python-and-curl/
#Cite: oauth2 module