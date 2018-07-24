import sys
import json
import logging
import datetime
import boto3
import math
import sqlalchemy
import hashlib
import os
import pandas as pd
import urllib.request
from mutagen.mp3 import MP3

logname = sys.argv[0]

logger = logging.getLogger(logname)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logger.setLevel(logging.INFO)

hdlr = logging.FileHandler('/var/tmp/' + logname + '.log')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 

stdout = logging.StreamHandler()
stdout.setFormatter(formatter)
logger.addHandler(stdout)

f = open('config.json', 'r')
j = f.read()
f.close()
o = json.loads(j)

db = o['db']
conn_template = 'mysql://{}:{}@{}:{}/{}'
connstr = conn_template.format(db['username'], db['password'], db['host'], db['port'], db['dbname'])
e = sqlalchemy.create_engine(connstr)


default_img = "https://s3.amazonaws.com/data-skeptic-bonus-feed/assets/bonus-img.png"

def get_metadata():
	n = datetime.datetime.now()
	lastBuildDate = n.strftime('%a, %d %b %Y %X %Z+0000')
	pubDate = lastBuildDate
	img = default_img
	return {"pubDate": pubDate, "lastBuildDate": lastBuildDate, "img": img}

def get_episodes(e, default_img):
	episodes = []
	query = """
		SELECT title, guid, link, pubDate, `desc`, img, enclosure_url 
		FROM dataskeptic.bonus_episodes 
		WHERE pubDate < now()
		ORDER BY pubDate desc
	"""
	df = pd.read_sql(query, e)
	return df

def get_item(episode, default_img, s):
	print(1)
	enclosure_url = episode['enclosure_url']
	print(enclosure_url)
	response = urllib.request.urlopen(enclosure_url)
	print(response)
	data = response.read()
	print(2)
	fname = '/tmp/file.mp3'
	f = open(fname, 'wb')
	f.write(data)
	f.close()
	print(3)
	audio = MP3(fname)
	minutes = audio.info.length / 60
	m = str(math.floor(minutes)).zfill(2)
	sec = str(math.floor((minutes - math.floor(minutes))*60)).zfill(2)
	print('Getting size')
	enclosure_len = os.path.getsize('/tmp/file.mp3')
	print('Done getting size')
	duration = m + ":" + sec
	desc = episode['desc']
	n =  episode['pubDate']
	pubDate = n.strftime('%a, %d %b %Y %X %Z+0000')
	if len(desc) < 230:
		subtitle = desc
	else:
		subtitle = desc[0:230] + "..."
	x = s.format(title=episode['title'], link=episode['link'], guid=episode['guid'], pubDate=pubDate, img=episode['img'], desc=episode['desc'], enclosure_url=episode['enclosure_url'], enclosure_len=enclosure_len, duration=duration, subtitle=subtitle)
	return x

def get_items(episodes, default_img):
	f = open('template_item.xml')
	s2 = f.read()
	f.close()
	arr = []
	for i in range(episodes.shape[0]):
		episode = episodes.iloc[i]
		item = get_item(episode, default_img, s2)
		arr.append(item)
	return arr

def generate_feed(metadata, episodes, fname):
	f = open('template.xml', 'r')
	s1 = f.read()
	f.close()
	pubDate = metadata['img']
	lastBuildDate = metadata['lastBuildDate']
	img = metadata['img']
	items = get_items(episodes, img)
	sitems = ""
	for item in items:
		sitems += item # update this, do formatting here
	feed = s1.format(pubDate=pubDate, lastBuildDate=lastBuildDate, img=img, items=sitems)
	f = open(fname, 'w')
	f.write(feed)
	f.close()

def upload_feed(fname):
	s3 = boto3.resource('s3')
	f = open(fname, 'r')
	data = f.read()
	f.close()
	s3.Bucket('data-skeptic-bonus-feed').put_object(Key=fname, ACL='public-read', Body=data)

if __name__ == '__main__':
	logger.info("Getting metadata")
	fname = 'data-skeptic-bonus.xml'
	metadata = get_metadata()
	logger.info("Getting episodes")
	episodes = get_episodes(e, default_img)
	logger.info("Generating feed")
	generate_feed(metadata, episodes, fname)
	logger.info("Uploading feed")
	upload_feed(fname)
	logger.info("Complete")
