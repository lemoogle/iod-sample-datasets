import json
import urllib
import requests
import json
import itertools
from iodpython.iodindex import IODClient
import shelve
from utils import FreebaseUtil

conf =shelve.open('../config.db')

freebase_key = conf['freebase-key']
iod_key=conf['iod-key']
service_url = 'https://www.googleapis.com/freebase/v1/mqlread'

sync=shelve.open('sync.db')

query =[{
    "name":None,
    "type": "/comic_books/comic_book_series",
    "limit": 30,
    "created_by":[{'name':None,'mid':None,'optional':True}],
    "publisher":[{'name':None,'mid':None,'optional':True}],
    "mid":None,
    "genre":[{'name':None,'mid':None,'optional':True}],
    "continues":[{'name':None,'mid':None,'optional':True}],
    "continued_by":[{'name':None,'mid':None,'optional':True}],
    "issues":[{'characters_on_cover':[{'name':None,'mid':None,'optional':True}],"optional":True}],
    #"issues":[{'characters_on_cover':[{}]}],
    "featured_characters":[{'name':None,'mid':None,'optional':True}],
    #'starring':[{"actor":None}],

}]






freebaseUtil = FreebaseUtil(freebase_key,query,description=True)

client = IODClient("http://api.idolondemand.com/",
                        iod_key)


#   client.deleteIndex('quotesdb')

# try: 
#   index=client.deleteIndex('comic_series')
# except:
#   pass

try:
  fields=["created_by_name","publisher_name","source_type_name","genre_name","continues_name","continued_by_name"]
  
  index=client.createIndex('comic_series',index_fields=fields, parametric_fields=fields)
  # index=client.createIndex('quotesdb',index_fields=["spoken_by_character_*","author_*","source_*","incorrectly_attributed_to_*","addressee_*"],parametric_fields=["spoken_by_character_*","author_*","source_*","incorrectly_attributed_to_*","addressee_*"])
  print "sleeping"
  time.sleep(5)
  print "resuming"
except:
    index=client.getIndex('comic_series')

#except:
##    print "getting instead"
#index= client.getIndex('comic_book_series')
#cursor = freebaseUtil.do_query(index)
cursor = sync.get("cursor","")

if not cursor:
  cursor = freebaseUtil.do_query(index)

while(cursor):
    cursor = freebaseUtil.do_query(index,cursor)
    sync["cursor"]=cursor






    