from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import iodindex
import re
import shelve

conf = shelve.open('../config.db')


class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.

    """
    index=None
    batch=None
    def __init__(self,index,batch=10):
        self.index=index
        self.batch=batch

    def on_data(self, data):
        data=json.loads(data)
        doc={}
        text=data["text"]
        text = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', text)
        doc["title"]=text
        doc["reference"]=str(data["id"])
        doc["content"]=""   
        #print self.index.client
        sentiment=self.index.client.post('analyzesentiment',{'text':text}).json()
        doc["category"]=sentiment["aggregate"]["sentiment"]
        index.pushDoc(doc)
        print "Pushing ",data["id"]
        if index.size()>self.batch:
            print "Committing"
            index.commit()

        return True

    def on_error(self, status):
        print status

if __name__ == '__main__':

    consumer_key = conf['twitter_consumer_key']
    consumer_secret = conf['twitter_consumer_secret']
    access_token =conf['twitter_access_token']
    access_token_secret = conf['twitter_access_token_secret']
    iod_key = conf['iod-key']

    client = iodindex.IODClient("http://api.idolondemand.com/",
                            iod_key)


    try:
        index=client.getIndex('twitter')
        index.delete()
    except:
        pass
    index=client.createIndex('twitter')


    l = StdOutListener(index,10)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['Great'])
    # Prompt for login credentials and setup stream object


