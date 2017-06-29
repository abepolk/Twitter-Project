#Find a way to save the tweet to disk

import json
import oauth2 as oauth
import os
import emoji as emo
import urllib.parse as parse

consumer_key = os.environ.get('CONSUMER_KEY')
consumer_secret = os.environ.get('CONSUMER_SECRET')

access_token = os.environ.get('ACCESS_TOKEN')
access_token_secret = os.environ.get('ACCESS_TOKEN_SECRET')

consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)
access_token = oauth.Token(key=access_token, secret=access_token_secret)
client = oauth.Client(consumer, access_token)

#This function will use an online ready-made dictionary to find the sentiment of an emoji
def get_emoji_sentiment(emoji):
    return 'sentiment'

emo_tweets = {}
for i in range(0, 3):
    params = {'q' : 'Katy Perry', 'count' : 100}
    # See Twitter Search API documenation for description of cursoring for below if block
    if i > 0:
        params['max_id'] = max_id 
    timeline_endpoint_baseuri = 'https://api.twitter.com/1.1/search/tweets.json?'
    timeline_endpoint = timeline_endpoint_baseuri + parse.urlencode(auth_params)
    print(timeline_endpoint)
    response, data = client.request(timeline_endpoint)
    tweets_results = json.loads(data)
    tweets_list = tweets_results['statuses']
    max_id = tweets_list[len(tweets_list) - 1]['id'] - 1
    for tweet in tweets_list:
        tweet_emojis = []
        tweet_sentiments = []
        for char in tweet['text']:
            if char in emo.UNICODE_EMOJI:
                tweet_emojis.append(char)
        for emoji in tweet_emojis:
            tweet_sentiments.append(get_emoji_sentiment(emoji))
        if tweet_emojis:
            emo_tweets[tweet['id']] = [tweet['text'], tweet_sentiments]
    print('Emoji tweets processed: ' + str(len(emo_tweets.keys())))
            
for tweet in emo_tweets.values():
    print(tweet)
    print()