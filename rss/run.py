

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
from iodpython.iodindex import IODClient



class MLStripper(HTMLParser):
	def __init__(self):
		self.reset()
		self.fed = []
	def handle_data(self, d):
		self.fed.append(d)
	def get_data(self):
		return '\n'.join(self.fed)



def process_source(source,syncdb,schedulesecs,database):
	count=0

	name=source["name"].encode('ascii','ignore')
	print "processing",name


	#create empty database to update old one.
	updatedb={}

	#Initialize articles Hash for storage of each article for source.	
	articles={}

	#Iterate through feeds
	for feed in source["feeds"]:

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
			ref=item[config["ref"]]

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


			if not article:

				#get and set values for all wanted mappings
				for key,value in config["fields"].items():
					if key in item.keys():
						article[value]=item[key]
				article["reference"]=ref
				#article[config["date"]]=time.asctime(item.published_parsed)
				article[config["date"]]=time.asctime(time.localtime(itemtime))
				#set source to source name
				article[config.get("source","source")]=name


				# get content from description, currently ignore fullhtml property.
				content_mapping= config.get("content","content")
				s = MLStripper()
				s.feed(item["summary"])
				article[content_mapping]=s.get_data()



				#Set or update the rss urls 
				article['rss_url']= list(set(article.get('rss_url',[])) | set([rss_url]))


			#get custom feed fields from source config
			custom_fields = feed.get("fields",[])

			# Get custom field mappings for standardization if needed
			custom_field_mappings=config.get("customfields",{})

			# Get default field mappings to map rss default fields to IOD default fields
			field_mappings=config.get("fields",{})

			# iterate through custom fields to add/append new values 
			for field in custom_fields:
				mappedfield=custom_field_mappings.get(field["name"],field["name"])
				article[mappedfield]= list(set(article.get(mappedfield,[])) | set(field["values"]))
			
			#set new/updated article
			articles[ref]=article

			#set last date to newest article's date, updatedb gets written in callback
		updatedb[rss_url]=templastdate
				#updatedb[rss_url]=time.time()


	# if articles this run then write them to idx
	if len(articles)>0:
		directory="output"
		if not os.path.exists(directory):
				os.makedirs(directory)	
		result = open(directory+"/"+source["name"].replace(" ","_")+str(int(time.time()))+".idx",'wb')
		

		for b in articles.values():
			count+=1
			index.pushDoc(b)
			if count>=20:
				try:
					print "trying to commit 1st try"
					index.commit()
				except:
					print "trying to commit 2nd try"
					index.commit()
				count=0

			#articleidx=json_to_idx_processor.toIDX(b).encode('utf-8')
			#result.write(articleidx)

	print "returning", name
	syncdb.update(updatedb);
	#callback_update({"name":name, "db":updatedb})


def start():
	while True:
		for source in sourcelist["sources"]:
			source_name=source["name"]
			#for each source add to pool of next to run jobs
			process_source(source,syncdb,schedulesecs,database)
		time.sleep(schedulesecs)
	#threading.Timer(schedulesecs,start).start()



# Load Mappings from json
config=json.loads(open('config.json').read())


syncdb=shelve.open("sync.db")

# Initiate Html stripper
sourcelist= json.loads(open('sources.json').read())
schedulesecs=config["schedulesecs"]
database=config["database"]

keyconf = shelve.open('../config.db')
iod_key=keyconf['iod-key']


client = IODClient("http://api.idolondemand.com/",
                        iod_key)
#try:
try:
	index=client.createIndex(database)
	print index
	time.sleep(3)
except:
	print "getting instead"
	index=client.getIndex(database)


def main():
	start()

if __name__ == "__main__":
	main()

#	time.sleep(5)


