import json
import urllib
import requests
import json
import itertools
from iodpython.iodindex import IODClient


api_key = "AIzaSyAorSwy_yAH6ObJPJb_q9mmYSlZch5XQII"
service_url = 'https://www.googleapis.com/freebase/v1/mqlread'

query =[{
    "name":None,
    "type": "/comic_books/comic_book_series",
    "limit": 10,
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




params = {
                'query': json.dumps(query),
                'key': api_key
}

freebase_topic_url="https://www.googleapis.com/freebase/v1/topic{}?filter=/common/topic/description&key={}"



#
# Note to self: This method sucks, make it better
#
#
def flatten(obj, key=""):
    if type(obj) is dict:
        orig=dict(obj)
        for k,v in obj.iteritems():
            #print k,v
            newkey=""
            if key:
                newkey=key+"_"
            newkey+=k
            if type(v) is dict:
                orig.update(flatten(v,newkey))
                orig.pop(k)
            elif type(v) is list:
                    flatlist=flatten(v,newkey);

                    if flatlist:
                        orig.update(flatlist)
                        orig.pop(k)
                    #print flatten(val,newkey)
                    #orig.update(flatten(v,newkey))
            else:
                if key:
                    orig[newkey]=v
                    orig.pop(k)
        return orig
    if type(obj) is list:
        new={}
        for a in obj:
            if type(a) is dict:

                flatlistitem=flatten(a,key)
                for k,v in flatten(a,key).iteritems():
                    #print new.get(k,[]).append(v) 
                    new[k]=new.get(k,[])
                    new[k].append(v)  


        if not new:
            return False
        return new
    return obj




#d= {'a':'val','nest':[{'name':'n','id':'i'},{'name':'n2','id':'i2'}]}
#d= {'a':'val','nest':{'id':'i'}}
#d= {'a':'val'}

def flattenlists(obj):
    for k,v in obj.iteritems():
        if type(v) is list and len(v)>0 and not isinstance(v[0], basestring):
            obj[k]=list(itertools.chain(*v))

    return obj

def do_query(index,cursor=""):

    params['cursor']=cursor
    
    url = service_url + '?' + urllib.urlencode(params)
    response = requests.get(url).json()#json.loads(urllib2.urlopen(url).read())

    for result in response['result']:
        #print result['mid']
        
        try:
            content = requests.get(freebase_topic_url.format(result["mid"],api_key)).json()
            content=content["property"]["/common/topic/description"]["values"][0]["value"]
            result["content"]=content
        except:
            pass
            #print result
            #print content, freebase_topic_url.format(result["mid"],api_key)
        result["reference"] = result.pop("mid")
        result["title"] = result.pop("name")
        #characters= result["issues"];
        #if characters:
        #    characters=map(lambda x: x.get('characters_on_cover',[]) ,characters )
        #    characters=reduce(lambda x, y: x+y, characters)
        #result["featured_characters"]+=characters
        #result.pop('issues')
        result= flatten(result)
        result= flattenlists(result)
        index.pushDoc(result)
        #print json.dumps(flatten(result),indent=4)
        #print result["continues"]
    print index.name
    try:
        print "trying to index"
        print index.commit().text
    except:
        print "indexing failed"
    return response.get("cursor")


client = IODClient("http://api.idolondemand.com/",
                        "1642237f-8d30-4263-b2f9-12efab36c779")
#try:
index=client.getIndex('comic_series')

#except:
##    print "getting instead"
#index= client.getIndex('comic_book_series')
cursor = do_query(index)
while(cursor):
    cursor = do_query(index,cursor)






    