# Twitter sentiment analysis project

## Introduction

A demonstration of a Naive Bayes (NB) sentiment analysis model.  The model is applied to hundreds of thousands of tweets containing the search term "Justin Bieber."  Each tweet containing emojis receives a "emoji sentiment score" based on the positivity or negativity of the emojis in the tweet.  To score each of these tweets, an "emoji sentiment dictionary" is used.  The dictionary was previously compiled (Kralj Novak, Petra; Smailović, Jasmina; Sluban, Borut; Mozetič, Igor, 2015) and is available at https://www.clarin.si/repository/xmlui/handle/11356/1048.  It is also copied into this repository.  The NB classifier is trained on the tweets with emojis.  After that, the model is k-fold cross-validated and tested on fake tweets coded into the script.

## Obtaining the tweets

The file twitter_streaming_data.py contains a script that connects to the Twitter Streaming API using the search term.  The script was deployed as a cloud-based app using the Heroku service.  The script first connects to the Twitter Streaming API using PycURL.  PycURL is Python's implementation of cURL, a tool in C that can be used to open a persistent HTTP connection to a server (here the Streaming API endpoint) and handle responses in a specified manner.  Error messages are handled and sent to a Papertrail on Heroku, a third-party app log service.  The text of tweets received is sent to mLab on Heroku, a third-party service providing cloud-based storage for MongoDB databases.  The size of the MongoDB is capped at 100 MB.

## Assignment of emoji scores

The analysis of the tweets was performed in the Jupyter notebook "Tweet analysis.ipynb."  The tweets first were downloaded from mLab.  Tweets were then checked for emojis, and each emoji in each tweet was given a score based on the positive or negative sentiment of the emoji as defined in the emoji sentiment dictionary.  Tweets without emojis were dropped.  For each tweet, the emoji with the score with the highest absolute value was chosen as the overall score for that tweet.  In other words, the most positive or most negative score was chosen, whichever sentiment was stronger.

## Tweet preprocessing and tokenizing

Retweets (beginning with "RT") were dropped.  Accents and URLs were removed from tweets, and tweets were lower-cased.  Apostrophes were standardized from different Unicode points.  Tweets were then tokenized by separating words out within each tweet into a list.

## Tweet vectorization and classification

Preprocessed and tokenized tweets were then converted into document vectors.  Tf-idf was not used.  The document vectors for each tweet were then used to train a Multinomial NB classifier.  The categories were created from buckets based on the emoji score.  When three_class is set to true, a neutral category was created for when emoji scores fell within a threshold of zero.  Otherwise, the categories were solely determined by the sign of the emoji score.  The cross-validation scores for the model were calculated, both in terms of accuracy and F-score.  The model was then tested on three fake tweets representing each category.