import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pprint
import re
import datetime
import pytz

from datetime import date
from datetime import datetime, timedelta
import time

from textblob import TextBlob
from textblob import Word

from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import nltk
#nltk.download('averaged_perceptron_tagger')
from nltk import word_tokenize, pos_tag

import twitter_credentials

import pdb

# # # TWITTER CLIENT # # # 
class TwitterClient():
    
    def __init__(self, twitter_user = None):
        self.auth = TwitterAuthenticater().authenticate_twitter_app()
        self.twitter_client = API(self.auth)
        
        self.twitter_user = twitter_user
        
    def get_twitter_client_api(self):
        return self.twitter_client
        
    def get_user_timeline_tweets(self, num_tweets):
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline, id = self.twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets
    
    def get_user_friend_list(self, num_friends):
        friend_list = []
        for friend in Cursor(self.twitter_client.friends, id = self.twitter_user).items(num_friends):
            friend_list.append(friend)
        return friend_list
    
    def get_home_timeline_tweets(self, num_tweets):
        home_timeline_tweets = []
        for tweet in Cursor(self.twitter_client.home_timeline_tweets, id = self.twitter_user).items(num_tweets):
            home_timeline_tweets.append(tweet)
        return home_timeline_tweets    
    
    def search_tweets (self, query, num_tweets, result_type):
        top_tweets = []
        for tweet in Cursor(self.twitter_client.search, q = query + ' -filter:retweets',  tweet_mode='extended', result_type = result_type).items(num_tweets):
            top_tweets.append(tweet)
        return top_tweets


# # # TWITTER AUTHENTICATER # # #
class TwitterAuthenticater():
    
    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth
        

# # # TWITTER STREAMER # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    
    def __init__(self):
        self.twitter_authenticater = TwitterAuthenticater()
    
    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter Authentication and the connection to the Twitter Streaming API.
        listener = TwitterListener(fetched_tweets_filename)
        auth = self.twitter_authenticater.authenticate_twitter_app()
        stream = Stream(auth, listener)
        
        stream.filter(track = hash_tag_list)
        
# # # TWITTER STREAM LISTENER # # #
class TwitterListener(StreamListener):
    """
    This is a basic listener class that prints received tweets to stdout.
    """
    
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename
    
    def on_data(self, data):
        try:
            #print(type(data))
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print('Error on_data:', str(e))
        return True
   
    def on_error(self, status):
        if status == 420:
            # Returns False on_data method in case rate limit occurs.
            return False
        print (status) 
  
# # # TWITTER ANALYSER # # #      
class TweetAnalyser():
    """
    Functionality for analysing and categorising content from tweets.
    """
    def tweets_to_dataframe(self, pre_tweets, ticker):
        current_datetime = datetime.utcnow()
        
        #pre_tweets = [tweet for tweet in pre_tweets if tweet.created_at.date() == current_datetime.date()]
    
        pre_market = datetime.strptime('11:30', '%H:%M')
        market_open = datetime.strptime('02:30', '%H:%M')
        lunch = datetime.strptime('16:30', '%H:%M')
        post_lunch = datetime.strptime('18:00', '%H:%M')
        after_market = datetime.strptime('20:30', '%H:%M')
        end_session = datetime.strptime('00:00', '%H:%M')
        
        if time.localtime().tm_isdst == 1:
            pre_market = pre_market + timedelta(hours=-1)
            market_open = market_open + timedelta(hours=-1)
            lunch = lunch + timedelta(hours=-1)
            post_lunch = post_lunch + timedelta(hours=-1)
            after_market = after_market + timedelta(hours=-1)
            end_session = end_session + timedelta(hours=-1)
            
        print('Open:', market_open.time())
        print('GMT:', current_datetime.time())
        
        tweets = []
        session = np.nan
        
        for i in range(0, len(pre_tweets)):
            if pre_market.time() < current_datetime.time() < market_open.time() and pre_market.time() < pre_tweets[i].created_at.time() < market_open.time():
                tweets.append(pre_tweets[i])
                session = 'Pre-market'
            elif market_open.time() < current_datetime.time() < lunch.time() and market_open.time() < pre_tweets[i].created_at.time() < lunch.time():
                tweets.append(pre_tweets[i])
                session = 'Open'
            elif lunch.time() < current_datetime.time() < post_lunch.time() and lunch.time() < pre_tweets[i].created_at.time() < post_lunch.time():
                tweets.append(pre_tweets[i])
                session = 'Lunch'
            elif post_lunch.time() < current_datetime.time() < after_market.time() and post_lunch.time() < pre_tweets[i].created_at.time() < after_market.time():
                tweets.append(pre_tweets[i])
                session = 'Post-lunch'
            elif after_market.time() < current_datetime.time() < end_session.time() and after_market.time() < pre_tweets[i].created_at.time() < end_session.time():
                tweets.append(pre_tweets[i])
                session = 'After-market'
            
            
        df = pd.DataFrame([tweet.full_text for tweet in tweets], columns = ['tweets'])
        
        #df['spellchecked_tweets'] =  np.array([TextBlob(tweet.full_text).correct() for tweet in tweets])
        df['id'] = np.array([tweet.id for tweet in tweets])
        df['len'] = np.array([len(tweet.full_text) for tweet in tweets])
        df['date'] = np.array([tweet.created_at for tweet in tweets])
        df['source'] = np.array([tweet.source for tweet in tweets])
        df['likes'] = np.array([tweet.favorite_count for tweet in tweets])
        df['retweets'] = np.array([tweet.retweet_count for tweet in tweets])
        df['ticker'] = ticker[1:]
        df['trading_cycle'] = session
        
        
        return df
    
    def clean_tweet(self, tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def analyse_sentiment_polarity(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        
        return analysis.sentiment.polarity
    
    def analyse_sentiment_subjectivity(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        
        return analysis.sentiment.subjectivity
        
    def determine_tense_input(self, sentence):
        text = word_tokenize(sentence)
        tagged = pos_tag(text)
    
        tense = {}
        tense["future"] = len([word for word in tagged if word[1] == "MD"])
        tense["present"] = len([word for word in tagged if word[1] in ["VBP", "VBZ","VBG"]])
        tense["past"] = len([word for word in tagged if word[1] in ["VBD", "VBN"]]) 
        return(tense)

class  TwitterSearch():
    
    def __init__(self):
        self.auth = TwitterAuthenticater.authenticate_twitter_app()
    
    def search_tweets(self):
        top_tweets = []
        #for tweet in Cursor(selft.)
        

if __name__ == '__main__':
     
    twitter_client = TwitterClient()
    tweet_analyser = TweetAnalyser()
    api = twitter_client.get_twitter_client_api()
    
    ticker = '$FB'
   
    popular_tweets = twitter_client.search_tweets(ticker, 50, 'popular')
    recent_tweets = twitter_client.search_tweets(ticker, 50, 'recent')
    

    recent_df = tweet_analyser.tweets_to_dataframe(recent_tweets, ticker)
    popular_df = tweet_analyser.tweets_to_dataframe(popular_tweets, ticker)
    
    if 'full_df' not in locals():
        full_df = recent_df
    else:
        full_df = full_df.append(recent_df)
        
    #full_df = full_df.append(popular_df)

    
    
    for index, row in popular_df.iterrows():
        if row['id'] not in full_df['id'].tolist():
            print('here')
            full_df = full_df.append(row, ignore_index=True)
        else:
            for index2, row2 in full_df.iterrows():
                print(index2)
                if row2['id'] == row['id']:
                    print('now here')
                    print(index)
                    print(index2)
                    full_df.iloc[index2, full_df.columns.get_loc('likes')] = row['likes']
                    full_df.iloc[index2, full_df.columns.get_loc('retweets')] = row['retweets']
                    # row2['likes'] = row['likes']
                    # row2['retweets'] = row['retweets']



    # df['polarity'] = np.array([tweet_analyser.analyse_sentiment_polarity(tweet) for tweet in df['tweets']])
    # df['subjectivity'] = np.array([tweet_analyser.analyse_sentiment_subjectivity(tweet) for tweet in df['tweets']])
    # df['tense'] = np.array([tweet_analyser.determine_tense_input(tweet) for tweet in df['tweets']])
    
    




    
    




    