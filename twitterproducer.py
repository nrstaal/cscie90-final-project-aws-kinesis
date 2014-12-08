#!env python
#To run this producer, you will have to create a Twitter app from
#apps.twitter.com. Once you have created an app, you will have to copy and paste
#the consumer key and consumer secret into the variables below. You will also 
#have to generate an access token and access token secret and insert those below
#as well.
#
#You will also have to create a kinesis stream. This logic is not provided in
#the twitter stream producer. Once you have created a kinesis stream, insert
#the stream name and region below. This script assumes the stream will only have
#one shard, as it is only a sample prototype script.
#

from __future__ import absolute_import, print_function
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from boto import kinesis
import sys

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key=""
consumer_secret=""

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token=""
access_token_secret=""

#Kinesis stream connection parameters
#Shard names would be dynamically created for applications looking to scale out the stream
conn = kinesis.connect_to_region(region_name = "us-east-1")
stream_name = ""
shard_name = "shard1"

def put_tweet_in_stream(tweet):
    """
    Put each tweet into the kinesis stream
    :type tweet: str
    """
    try:
        tweet_text = json.loads(tweet)['text']
        conn.put_record(stream_name,tweet,shard_name)
    except Exception as e:
        sys.stderr.write("Encountered an exception while trying to put a tweet: "
                         + tweet + " into stream: " + stream_name + " exception was: " + str(e))

class PutKinesisListener(StreamListener):
    """ A listener handles tweets received from the stream.
    This is a basic listener that puts received tweets to the Kinesis stream.
    """
    def on_data(self, data):
        put_tweet_in_stream(data)
        return True
    
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = PutKinesisListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.sample()

