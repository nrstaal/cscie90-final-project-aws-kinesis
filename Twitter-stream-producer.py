import sys
from __future__ import absolute_import, print_function
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from boto import kinesis

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
        conn.put_record(stream_name,tweet,shard_name)
    except Exception as e:
        sys.stderr.write("Encountered an exception while trying to put a tweet: "
                         + tweet + " into stream: " + stream_name + " exception was: " + str(e))

class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        put_tweet_in_stream(data)
        return True
    
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.sample()
