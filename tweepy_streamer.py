import pandas as pd
import numpy as np
import re
import sqlite3

import datetime
from datetime import datetime
from datetime import timedelta
import time

from textblob import TextBlob
from textblob import Word

from tweepy import API
from tweepy import Cursor
from tweepy import OAuthHandler

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


  
# # # TWITTER ANALYSER # # #      
class TweetAnalyser():
    """
    Functionality for analysing and categorising content from tweets.
    """
    def tweets_to_dataframe(self, pre_tweets, ticker):
        current_datetime = datetime.utcnow()
        
        # Remove tweets that were not posted today
        #pre_tweets = [tweet for tweet in pre_tweets if tweet.created_at.date() == current_datetime.date()]
    
        # Set trading phase times
        """pre_market = datetime.strptime('11:30', '%H:%M')
        market_open = datetime.strptime('14:30', '%H:%M')
        lunch = datetime.strptime('16:30', '%H:%M')
        post_lunch = datetime.strptime('18:00', '%H:%M')
        after_market = datetime.strptime('20:30', '%H:%M')
        end_session = datetime.strptime('00:00', '%H:%M')
        
        # If day light saving is on move time back by 1 hour
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
        
        # add tweet to tweets list if tweet was postited in a trading phase and also if we are currently in that trading phase
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
                session = 'After-market'"""
        tweets = pre_tweets
        # Create DataFrame of tweets and add columns
        df = pd.DataFrame([tweet.full_text for tweet in tweets], columns = ['tweets'])
        df['id'] = np.array([tweet.id for tweet in tweets])
        df['len'] = np.array([len(tweet.full_text) for tweet in tweets])
        df['date'] = np.array([tweet.created_at for tweet in tweets])
        df['source'] = np.array([tweet.source for tweet in tweets])
        df['likes'] = np.array([tweet.favorite_count for tweet in tweets])
        df['retweets'] = np.array([tweet.retweet_count for tweet in tweets])
        df['ticker'] = ticker[1:]
        #df['trading_cycle'] = session
        
        return df

    #Clean tweets before applying TextBlob
    def clean_tweet(self, tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())


    # Calculate polarity of tweet
    def analyse_sentiment_polarity(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        
        return analysis.sentiment.polarity
    
    
    # Calculate subjectivity of tweet
    def analyse_sentiment_subjectivity(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        
        return analysis.sentiment.subjectivity
    
    
    # Calculate tense of tweet    
    def determine_tense_input(self, sentence):
        text = word_tokenize(sentence)
        tagged = pos_tag(text)
    
        tense = {}
        tense["future"] = len([word for word in tagged if word[1] == "MD"])
        tense["present"] = len([word for word in tagged if word[1] in ["VBP", "VBZ","VBG"]])
        tense["past"] = len([word for word in tagged if word[1] in ["VBD", "VBN"]]) 
        return(tense)
    


# # # TWITTER ANALYSER # # #      
class ImportToDB():
    """
    Import dataframe to DB
    """
    
    def to_dataframe(self, popular_df):
        # Check if full dataframe object exists, if not initiate dataframe
        if 'full_df' not in locals():
            full_df = popular_df
        else:
            full_df = full_df.append(popular_df)
        
        # Add popular tweets to full_df, replace likes and retweets if popular tweet already exists in full_df
        """for index, row in popular_df.iterrows():
            if row['id'] not in full_df['id'].tolist():
                full_df = full_df.append(row, ignore_index=True)
            else:
                for index2, row2 in full_df.iterrows():
                    if row2['id'] == row['id']:
                        full_df.iloc[index2, full_df.columns.get_loc('likes')] = row['likes']
                        full_df.iloc[index2, full_df.columns.get_loc('retweets')] = row['retweets']"""
        
        return full_df
    
    def to_database(self, full_df):
        conn = sqlite3.connect('buytherumour.db')
        c = conn.cursor()
        
        full_df.to_sql(name='full_twitter_scrape', con=conn, if_exists='append')
        # pd.read_sql('select * from full_twitter_scrape', conn)


        

if __name__ == '__main__':
     
    twitter_client = TwitterClient()
    tweet_analyser = TweetAnalyser()
    import_to_db = ImportToDB()
    api = twitter_client.get_twitter_client_api()
    
    tickers = ['$FB', '$AAPL', '$AMZN', '$GOOGL', '$MSFT', '$TSLA', '$UBER', '$NFLX', '$BABA']
    
    for ticker in tickers:
        print(ticker)
        # Get tweets from Twitter API
        popular_tweets = twitter_client.search_tweets(ticker, 500, 'popular')
        #recent_tweets = twitter_client.search_tweets(ticker, 500, 'recent')
    
        # Process tweets to DataFrame
        #recent_df = tweet_analyser.tweets_to_dataframe(recent_tweets, ticker)
        popular_df = tweet_analyser.tweets_to_dataframe(popular_tweets, ticker)
        
        full_df = import_to_db.to_dataframe(popular_df)
        
        # Apply NLP on full_df
        full_df['polarity'] = np.array([tweet_analyser.analyse_sentiment_polarity(tweet) for tweet in full_df['tweets']])
        full_df['subjectivity'] = np.array([tweet_analyser.analyse_sentiment_subjectivity(tweet) for tweet in full_df['tweets']])
        #full_df['tense'] = np.array([tweet_analyser.determine_tense_input(tweet) for tweet in full_df['tweets']])
        
        import_to_db.to_database(full_df)
        
        time.sleep(60)
    
    
    
    
    
    
    




    
    



