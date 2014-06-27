#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function,division
import datetime
from bson import json_util
import os, yaml, codecs, random, json
from argparse import ArgumentParser

from convert_timestamp import convert_timestamp
import pymongo

client = pymongo.MongoClient()
db = client['wcTweets']
collection = db['tweets']

def getDump(start, end, dump_location):
    # tweets = list(collection.find({'timestamp': { '$gte': start, '$lte': end}}).sort([('timestamp', pymongo.DESCENDING)]))
    tweets = list(collection.find({'timestamp': { '$gte': start, '$lte': end}, 'coordinates': { '$ne': None }}).sort([('timestamp', pymongo.ASCENDING)]))
    # random selection
    sample_size = 1000
    tweets = [ tweets[i] for i in sorted(random.sample(xrange(len(tweets)), sample_size)) ]
    # TODO: add filters
    with codecs.open(dump_location, 'w', 'utf8') as output:
	# to use this format, do:
        # json.loads(aJsonString, object_hook=json_util.object_hook)
        output.write(json.dumps(tweets, default=json_util.default, indent=4))
#       for tweet in tweets:
            #output.write('TEXT: {}\n\tTIMESTAMP: {}'.format(tweet['text'], str(tweet['timestamp'])))
            #output.write('TEXT: '+ tweet['text'] + '\n\tTIMESTAMP: ' + str(tweet['timestamp']) + ' COOR: ' + str(tweet['coordinates']) + '\n\tPLACE: ' + str(tweet['place']) + '\n')
#            output.write(json.dumps(tweet, default=json_util.default))
            
if __name__ == '__main__':

    # get the command line options
    parser = ArgumentParser()
    parser.add_argument("configuration_file", action="store", help="path to the config file (in YAML format).")
    args = parser.parse_args()

    cfg_path = args.configuration_file

    config = None
    start = None
    end = None
    dump_location = None

    now = datetime.datetime.utcnow()
    yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    # read configuration file
    with open(cfg_path, "r") as cfg_file:
        config = yaml.load(cfg_file.read())
        start = config.get('start', None)
        end = config.get('end', None)
        dump_location = config.get('dump_location', os.getcwd() + '/tweet_dump.out')
        # dump_location = config.get('dump_location', '/home/chris/projects/twitter/twitter_interface/tweet_retriever/tweet_dump.out')

    if start is not None:
        start = convert_timestamp(start)
    else:
        print('start is none')
        start = yesterday

    if end is not None:
        end = convert_timestamp(end)
    else:
        print('end is none')
        end = now

    getDump(start, end, dump_location)
