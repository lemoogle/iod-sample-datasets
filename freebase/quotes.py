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



query= [{
  "mid": None,
  "name": None,
  "limit":30,
  "type": "/media_common/quotation",
  "author": [{
    "name": None,
    "mid": None,
    "profession":[],
    "optional": True
  }],
  "date": None,
  "spoken_by_character": [{
    "name": None,
    "mid": None,
    "optional": True
  }],
  "source": [{
    "name": None,
    "type":[{"name":None}],
    "mid": None,
    "optional": True
  }],
  "addressee": [{
    "name": None,
    "mid": None,
    "optional": True
  }],
  "incorrectly_attributed_to": [{
    "name": None,
    "mid": None,
    "optional": True
  }],
  "subjects": []
}]







freebaseUtil = FreebaseUtil(freebase_key,query)

client = IODClient("http://api.idolondemand.com/",
                        iod_key)


#   client.deleteIndex('quotesdb')

try:
    fields=["spoken_by_character_name","author_name","author_profession","source_type_name","source_name","incorrectly_attributed_to_name","addressee_name"]
    index=client.createIndex('quotesdb',index_fields=fields, parametric_fields=fields)
   # index=client.createIndex('quotesdb',index_fields=["spoken_by_character_*","author_*","source_*","incorrectly_attributed_to_*","addressee_*"],parametric_fields=["spoken_by_character_*","author_*","source_*","incorrectly_attributed_to_*","addressee_*"])

except:
    index=client.getIndex('quotesdb')

#except:
##    print "getting instead"
#index= client.getIndex('comic_book_series')
cursor = freebaseUtil.do_query(index)
while(cursor):
    print cursor
    cursor = freebaseUtil.do_query(index,cursor)






    