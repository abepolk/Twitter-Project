import json
import oauth2 as oauth
import os

consumer_key = os.environ.get('CONSUMER_KEY')
consumer_secret = os.environ.get('CONSUMER_SECRET')
access_token = os.environ.get('ACCESS_TOKEN')
access_token_secret = os.environ.get('ACCESS_TOKEN_SECRET')

consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)
access_token = oauth.Token(key=access_token, secret=access_token_secret)
client = oauth.Client(consumer, access_token)

timeline_endpoint = "https://api.twitter.com/1.1/geo/id/df51dec6f4ee2b2c.json"

#print(consumer_key + " " + consumer_secret + " " + access_token + " " + access_token_secret)

response, data = client.request(timeline_endpoint)

geo = json.loads(data)
print(geo.values())
#for tweet in tweets:
 #   print(data)