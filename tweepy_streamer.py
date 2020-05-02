import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pprint
import re
import datetime
import pytz

from datetime import date
from datetime import datetime#, timedelta

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
    def tweets_to_dataframe(self, pre_tweets):
        #pre_tweets = [tweet for tweet in pre_tweets if tweet.created_at.date() == datetime.now().date()]
    
        pre_market = datetime.strptime('06:30', '%H:%M')
        market_open = datetime.strptime('09:30', '%H:%M')
        lunch = datetime.strptime('11:30', '%H:%M')
        post_lunch = datetime.strptime('13:00', '%H:%M')
        after_market = datetime.strptime('15:30', '%H:%M')
        end_session = datetime.strptime('19:00', '%H:%M')
        
        tweets = []
        session = ''
        
        #nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern')) tweets = [tweet for tweet in pre_tweets if pre_market.time() < datetime.now().time() < market_open.time()]
        for i in range(0, len(pre_tweets)):
            print('here')
            if pre_market.time() < datetime.now().time() < market_open.time() and pre_market.time() < pre_tweets[i].created_at.time() < market_open.time():
                tweets.append(pre_tweets[i])
                session = 'Pre-market'
            elif market_open.time() < datetime.now().time() < lunch.time() and pre_market.time() < pre_tweets[i].created_at.time() < market_open.time():
                tweets.append(pre_tweets[i])
                session = 'Open'
            elif lunch.time() < datetime.now().time() < post_lunch.time() and pre_market.time() < pre_tweets[i].created_at.time() < market_open.time():
                tweets.append(pre_tweets[i])
                session = 'Lunch'
            elif post_lunch.time() < datetime.now().time() < after_market.time() and pre_market.time() < pre_tweets[i].created_at.time() < market_open.time():
                tweets.append(pre_tweets[i])
                session = 'Post-lunch'
            elif after_market.time() < datetime.now().time() < end_session.time() and pre_market.time() < pre_tweets[i].created_at.time() < market_open.time():
                tweets.append(pre_tweets[i])
                session = 'After-market'
            else:
                session = np.nan
                
            
            
        df = pd.DataFrame([tweet.full_text for tweet in tweets], columns = ['tweets'])
        
        #df['spellchecked_tweets'] =  np.array([TextBlob(tweet.full_text).correct() for tweet in tweets])
        df['id'] = np.array([tweet.id for tweet in tweets])
        df['len'] = np.array([len(tweet.full_text) for tweet in tweets])
        df['date'] = np.array([tweet.created_at for tweet in tweets])
        df['source'] = np.array([tweet.source for tweet in tweets])
        df['likes'] = np.array([tweet.favorite_count for tweet in tweets])
        df['retweets'] = np.array([tweet.retweet_count for tweet in tweets])
        df['trading_cycle'] = session
        
        #Old Delete
        """pre_market = datetime.strptime('07:30', '%H:%M')
        market_open = datetime.strptime('09:30', '%H:%M')
        lunch = datetime.strptime('11:30', '%H:%M')
        post_lunch = datetime.strptime('13:00', '%H:%M')
        after_market = datetime.strptime('15:30', '%H:%M')
        end_session = datetime.strptime('19:00', '%H:%M')
        
        #nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern'))
        for i in range(0, len(df.index)):
            if pre_market.time() < df.iloc[i, df.columns.get_loc('date')].time() < market_open.time():
                df.iloc[i, df.columns.get_loc('trading_cycle')] = 'Pre-market'
            elif market_open.time() < df.iloc[i, df.columns.get_loc('date')].time() < lunch.time():
                df.iloc[i, df.columns.get_loc('trading_cycle')] = 'Open'
            elif lunch.time() < df.iloc[i, df.columns.get_loc('date')].time() < post_lunch.time():
                df.iloc[i, df.columns.get_loc('trading_cycle')] = 'Lunch'
            elif post_lunch.time() < df.iloc[i, df.columns.get_loc('date')].time() < after_market.time():
                df.iloc[i, df.columns.get_loc('trading_cycle')] = 'Post-lunch'
            elif after_market.time() < df.iloc[i, df.columns.get_loc('date')].time() < end_session.time():
                df.iloc[i, df.columns.get_loc('trading_cycle')] = 'After-market'"""
        
        
        return df
    
    def clean_tweet(self, tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def analyse_sentiment_polarity(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        
        # Return binary polarity
        """if analysis.sentiment.polarity > 0:
            return 1
        elif analysis.sentiment.polarity == 0:
            return 0
        else:
            return -1"""
        
        return analysis.sentiment.polarity
    
    def analyse_sentiment_subjectivity(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        
        # Return binary polarity
        """if analysis.sentiment.subjectivity > 0.5:
            return 1
        else:
            return -1"""
        
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
   
    #tweets = api.user_timeline(screen_name = 'Sheldon08638921', count = 200)
    #popular_tweets = twitter_client.search_tweets('$fb', 500, 'popular')
    #recent_tweets = twitter_client.search_tweets('$fb', 500, 'recent')
    
    #popular_tweets_today = [tweet for tweet in popular_tweets if tweet.created_at.strftime("%Y-%m-%d") == date.today().strftime('%Y-%m-%d')]
    
    

    df = tweet_analyser.tweets_to_dataframe(popular_tweets)
    
    # Time Series  
    """time_likes = pd.Series(data = df['likes'].values, index = df['date'])
    time_likes.plot(figsize = (16, 4), label = 'likes', legend = True)
    time_retweets = pd.Series(data = df['retweets'].values, index = df['date'])
    time_retweets.plot(figsize = (16, 4), label = 'retweets', legend = True)
    plt.show()"""

    df['polarity'] = np.array([tweet_analyser.analyse_sentiment_polarity(tweet) for tweet in df['tweets']])
    df['subjectivity'] = np.array([tweet_analyser.analyse_sentiment_subjectivity(tweet) for tweet in df['tweets']])
    df['tense'] = np.array([tweet_analyser.determine_tense_input(tweet) for tweet in df['tweets']])
    
    




    
    
    
    
    
    
    
# pd.set_option('display.max_colwidth', 50)

"""hash_tag_list = ['donald trump', 'hillary clinton', 'barack obama', 'bernie sanders']
fetched_tweets_filename = 'tweets.json'
twitter_streamer = TwitterStreamer()
twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)
twitter_client =  TwitterClient('pycon')
print(twitter_client.get_user_timeline_tweets(1))
print(twitter_client.get_user_friend_list(1))"""
    
    
    