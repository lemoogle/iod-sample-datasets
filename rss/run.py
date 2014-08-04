

import ConfigParser,commands 
import feedparser
from multiprocessing import Pool
import threading
import sys
import os
import signal
import shelve
import json
import time
from sets import Set
from HTMLParser import HTMLParser
import urllib2


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ' '.join(self.fed)




class JsonToIDX:

        def __init__(self,mappings):
                self.reference=mappings["ref"]
        def toIDX(self,json):
		#print json
                result=""
                result+= '#DREREFERENCE %s\n' % (json[self.reference])
                del json[self.reference]
                for a,b in json.items():
                        if  type(b)==list:
                                for el in b:
                                        result+= '#DREFIELD %s="%s"\n' % (a,el.replace('"', '\\"'))
                        else:
                                result+= '#DREFIELD %s="%s"\n' % (a,b.replace('"', '\\"'))
		
                result+="#DREENDDOC\n\n"
		return result


#Callback to be called after every run
def callback_update(result):
	syncdb.update(result['db'])

	del pool_results[result['name']]
	
def process_source(source,syncdb,schedulesecs,database):
	
	name=source["name"].encode('ascii','ignore')
        print "processing",name


	#create empty database to update old one.
	copydb={}

	#Fetch options for specified source
	options =source["options"]
	
	#Initialize articles Hash for storage of each article for source.	
	articles={}

	#Iterate through feeds
	for feed in source["feeds"]:
		#lastpubdate=feedobject["lastpubdate"]
		#cats=feedobject["categories"]
		#Fetch feed using feedparser
		feed_parsed = feedparser.parse(feed["rss_url"])
		if len(feed_parsed["items"])==0:
			continue;
		#Initialise tempdate to find last edited time based on articles.
		templastdate=0

		#Get last published date from the last time it was checked
		rss_url=feed["rss_url"].encode('ascii','ignore').replace("\n","")

		#Get last updated time of feed from previous runs if exists.
		lastpubdate=0
		if rss_url in syncdb:
			lastpubdate=syncdb[rss_url]	
	
		#Iterate through each item/article from the feed
		for item in feed_parsed["items"]:
			
			# Get reference, use mappings to check for what field to use as reference
			ref=item[mappings["ref"]]
			# Compare time of article with last published date checked. Discard if older

			
			if item.get('published_parsed',False):
				itemtime=time.mktime(item.published_parsed)
			else:
				itemtime=time.time()
			if templastdate<itemtime:
				templastdate=itemtime

			#if article was older than newest article of last run, ignore
			if lastpubdate>=itemtime:
				continue;


			#Get article if seen already for this source, else create
			article = articles.get(ref,{})
			#if article is new
			if article == {}:


				#get and set values for all wanted mappings
				for key,value in mappings["fields"].items():
					if key in item.keys():
						article[value]=item[key]
				
				#set date
				#article[mappings["date"]]=time.asctime(item.published_parsed)
				article[mappings["date"]]=time.asctime(time.localtime(itemtime))
				#set source to source name
				article[mappings.get("source","source")]=name


				# get content from description, currently ignore fullhtml property.
				content_mapping= mappings.get("content","content")
				if options.get("fullhtml",True):
					s = MLStripper()
					#s.feed(item["summary"])
					#article[content_mapping]=s.get_data()
				else:
					article[content_mapping]=item["summary"]



			#Set or update the rss urls 
            		article['rss_url']= list(set(article.get('rss_url',[])) | set([rss_url]))


			#get custom feed fields from source config
			custom_fields = feed.get("fields",[])

			# Get custom field mappings for standardization if needed
			custom_field_mappings=mappings.get("customfields",{})

			# Get default field mappings to map rss default fields to IOD default fields
			field_mappings=mappings.get("fields",{})

			# iterate through custom fields to add/append new values 
			for field in custom_fields:
				mappedfield=custom_field_mappings.get(field["name"],field["name"])
				article[mappedfield]= list(set(article.get(mappedfield,[])) | set(field["values"]))
            

			#set new/updated article
            		articles[ref]=article

        	#set last date to newest article's date, copydb gets written in callback
		copydb[rss_url]=templastdate
                #copydb[rss_url]=time.time()


	# if articles this run then write them to idx
	if len(articles)>0:
		directory="output"
		if not os.path.exists(directory):
    			os.makedirs(directory)	
		result = open(directory+"/"+source["name"].replace(" ","_")+str(int(time.time()))+".idx",'wb')
		for b in articles.values():
			articleidx=json_to_idx_processor.toIDX(b).encode('utf-8')
			result.write(articleidx)
	
		try:
			urllib2.urlopen("http://localhost:9001/DREADD?"+ os.path.realpath(result.name)+"&DREDBName="+database+"&delete=true").read()
		except:
			copydb={}
                result.close()
		

	print "returning", name
	return {"name":name, "db":copydb}


#for catchign keyboard interups
def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def start():
        try:
			while True:
				for source in config["sources"]:
					source_name=source["name"]
					#for each source add to pool of next to run jobs
					if not source_name in pool_results:
						pool_results[source_name]=p.apply_async(process_source,[source,dict(syncdb),schedulesecs,database],callback=callback_update)
					else:
						print "skipping, not done, ", source_name
				time.sleep(schedulesecs)

		#threading.Timer(schedulesecs,start).start()
        except KeyboardInterrupt:
            print "Caught KeyboardInterrupt, terminating workers"
            p.terminate()
            sys.exit()


# Load Mappings from json
mappings=json.loads(open('rssmapping.json').read())

json_to_idx_processor = JsonToIDX(mappings)


syncdb=shelve.open("sync.db")


# Initiate Html stripper
config= json.loads(open('rsscfg.json').read())
schedulesecs=config["options"]["schedulesecs"]
database=config["database"]["name"]
threads=config["threads"]
p = Pool(threads, init_worker)


def signal_handler(a,b):
	p.terminate();
	sys.exit()

signal.signal(signal.SIGTERM, signal_handler)

	
pool_results={}
def main():
	start()

if __name__ == "__main__":
    main()

#	time.sleep(5)


