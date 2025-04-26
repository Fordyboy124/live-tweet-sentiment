# twitter_stream.py - Updated for Twitter API v2

import tweepy
import os
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TwitterStream:
    def __init__(self, hashtag="#GrowWithGroqHack"):
        """Initialize the Twitter stream with API credentials and target hashtag"""
        
        # Get credentials from environment variables for security
        self.bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
        self.api_key = os.environ.get("TWITTER_API_KEY")
        self.api_secret = os.environ.get("TWITTER_API_SECRET")
        self.access_token = os.environ.get("TWITTER_ACCESS_TOKEN")
        self.access_token_secret = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")
        
        # Validate credentials are available
        self._validate_credentials()
        
        # Initialize Twitter client
        self.client = self._initialize_client()
        
        # Search parameters
        self.hashtag = hashtag
        self.query = f"{hashtag} -is:retweet"  # Exclude retweets
        
        logging.info(f"TwitterStream initialized for hashtag: {hashtag}")
    
    def _validate_credentials(self):
        """Validate that required credentials are available"""
        # For v2 API, we can use either bearer token or OAuth 1.0a
        if not self.bearer_token and not (self.api_key and self.api_secret and 
                                         self.access_token and self.access_token_secret):
            missing = []
            if not self.bearer_token:
                missing.append("TWITTER_BEARER_TOKEN")
            if not self.api_key:
                missing.append("TWITTER_API_KEY")
            if not self.api_secret:
                missing.append("TWITTER_API_SECRET")
            if not self.access_token:
                missing.append("TWITTER_ACCESS_TOKEN")
            if not self.access_token_secret:
                missing.append("TWITTER_ACCESS_TOKEN_SECRET")
            
            raise ValueError(f"Missing required Twitter API credentials. Need either TWITTER_BEARER_TOKEN or OAuth credentials: {', '.join(missing)}")
    
    def _initialize_client(self):
        """Initialize and return the Tweepy client for v2 API"""
        try:
            # First try with bearer token (preferred for v2 API)
            if self.bearer_token:
                client = tweepy.Client(bearer_token=self.bearer_token)
                logging.info("Twitter API v2 client initialized with Bearer Token")
                return client
            
            # Fall back to OAuth 1.0a if no bearer token
            client = tweepy.Client(
                consumer_key=self.api_key,
                consumer_secret=self.api_secret,
                access_token=self.access_token,
                access_token_secret=self.access_token_secret
            )
            logging.info("Twitter API v2 client initialized with OAuth 1.0a")
            return client
            
        except Exception as e:
            logging.error(f"Error initializing Twitter v2 client: {str(e)}")
            raise
    
    def get_recent_tweets(self, count=10):
        """Fetch recent tweets matching the hashtag using Twitter API v2"""
        try:
            # Define tweet fields we want to retrieve
            tweet_fields = ['created_at', 'public_metrics', 'text']
            user_fields = ['username', 'name', 'profile_image_url']
            expansions = ['author_id']
            
            # Search for tweets
            response = self.client.search_recent_tweets(
                query=self.query,
                max_results=count,
                tweet_fields=tweet_fields,
                user_fields=user_fields,
                expansions=expansions
            )
            
            if not response.data:
                logging.warning(f"No tweets found matching query: {self.query}")
                return []
            
            # Process tweets into standardized format
            processed_tweets = []
            
            # Create a dict of users for quick lookup
            users = {user.id: user for user in response.includes['users']} if 'users' in response.includes else {}
            
            for tweet in response.data:
                # Get user info
                user = users.get(tweet.author_id)
                username = f"@{user.username}" if user else "Unknown User"
                
                # Format timestamp
                timestamp = tweet.created_at.strftime("%Y-%m-%d %H:%M:%S") if tweet.created_at else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                processed_tweet = {
                    "id": tweet.id,
                    "username": username,
                    "text": tweet.text,
                    "timestamp": timestamp,
                    "user_followers": getattr(user, 'public_metrics', {}).get('followers_count', 0) if user else 0,
                    "retweet_count": tweet.public_metrics.get('retweet_count', 0) if hasattr(tweet, 'public_metrics') else 0,
                    "like_count": tweet.public_metrics.get('like_count', 0) if hasattr(tweet, 'public_metrics') else 0
                }
                processed_tweets.append(processed_tweet)
            
            logging.info(f"Fetched {len(processed_tweets)} tweets with hashtag {self.hashtag}")
            return processed_tweets
            
        except Exception as e:
            logging.error(f"Error fetching tweets: {str(e)}")
            return []
    
    def get_stream_sample(self):
        """Get a single tweet from the stream - can be called repeatedly for streaming effect"""
        tweets = self.get_recent_tweets(count=5)
        
        if not tweets:
            return None
        
        # Return most recent tweet first
        return tweets[0] if tweets else None
