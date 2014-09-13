import json
import urllib
import requests 

def flatten(obj, key=""):
    if type(obj) is dict:
        orig=dict(obj)
        for k,v in obj.iteritems():
            #print k,v
            newkey=""
            if key:
                newkey=key+""
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


def flattenlists(obj):
    for k,v in obj.iteritems():
        if type(v) is list and len(v)>0 and not isinstance(v[0], basestring):
            obj[k]=list(itertools.chain(*v))

    return obj


class FreebaseUtil(object):

    freebase_topic_url="https://www.googleapis.com/freebase/v1/topic{}?filter=/common/topic/description&key={}"

    service_url = 'https://www.googleapis.com/freebase/v1/mqlread'

    def __init__(self,freebase_key,query,description=False):
        self.description=description
        self.params = {
                'query': json.dumps(query),
                'key': freebase_key
        }

    def do_query(self,index,cursor=""):

        self.params['cursor']=cursor
        
        url = self.service_url + '?' + urllib.urlencode(self.params)
        response = requests.get(url).json()#json.loads(urllib2.urlopen(url).read())
        for result in response['result']:
            #print result['mid']
            
            if self.description:
                try:
                    freebase_url=self.freebase_topic_url.format(result["mid"],self.params["key"])
                    content = requests.get(freebase_url).json()
                    content=content["property"]["/common/topic/description"]["values"][0]["value"]
                    result["content"]=content
                except:
                    pass
                    #print result
                    #print content, freebase_topic_url.format(result["mid"],api_key)
            else:
                result["content"]=""

            result["reference"] = result.pop("mid")
            result["title"] = result.pop("name")
            #characters= result["issues"];
            #if characters:
            #    characters=map(lambda x: x.get('characters_on_cover',[]) ,characters )
            #    characters=reduce(lambda x, y: x+y, characters)
            #result["featured_characters"]+=characters
            #result.pop('issues')
            try:
                result= flatten(result)
                result= flattenlists(result)
            except:
                pass
            #print result
            if "authorname" in result:
                result["category"]=result["authorname"]
            index.pushDoc(result)
            #print json.dumps(flatten(result),indent=4)
            #print result["continues"]
        print index.name
        try:
            print "trying to index"
            print index.commit(async=True).jobID
        except:
            print "indexing failed"

        return response.get("cursor")

