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

def getTimeDump(start, end, dump_location):
    # tweets = list(collection.find({'timestamp': { '$gte': start, '$lte': end}}).sort([('timestamp', pymongo.DESCENDING)]))
    tweets = list(collection.find({'timestamp': { '$gte': start, '$lte': end}, 'coordinates': { '$ne': None }}).sort([('timestamp', pymongo.ASCENDING)]))
    # random selection
    sample_size = 1000
    tweets = [ tweets[i] for i in sorted(random.sample(xrange(len(tweets)), sample_size)) ]
    # add filters?
    with codecs.open(dump_location, 'w', 'utf8') as output:
	# to use this format, do:
        # json.loads(aJsonString, object_hook=json_util.object_hook)
        output.write(json.dumps(tweets, default=json_util.default, indent=4))


# TODO: add real sentiment query -- depends on sentiment analysis code
def get_sentiment(text):
    return 3.0

# WORKING
# STEPS
# 1 - get tweets by timestamp, group by timestamp
# 2 - for each tweet, calculate sentiment relative to entities in configuration, store into 'entity_tweets'
#     --> WORKING - see Jian's code for this

# 3 - create a new mongo collection with 'match_id',
# match_obj is { 'matchName', 'time': { start_time: '', end_time: '' }} --> times are in the format: 'Thu Jun 12 16:08:42 +0000 2014'
# def create_match_with_sentiment(match_obj):
def get_tweets_in_window(match_obj):
    match_name = match_obj['matchName']
    start_time = convert_timestamp(match_obj['time']['startTime'])
    end_time = convert_timestamp(match_obj['time']['endTime'])

    # $match, $project the minute substring, then $group by minute
    match_tweets = collection.aggregate([{'$match': { 'timestamp': { '$gte': start_time, '$lte': end_time }}},
                                              {'$project': { 'minute': {'$substr': ['$timestamp', 0, 16]}, 'text': 1 }},
                                              {'$group': { '_id': '$minute', 'tweets': { '$push': { 'text': '$text' }}}}])

    tweets_with_sentiment = []
    for minute in match_tweets['result']:
        avg_sentiment = sum([get_sentiment(t['text']) for t in minute['tweets'] ]) / len(minute['tweets'])
        tweet_sentiments = [ { 'text': t['text'], 'sentiment': get_sentiment(t['text']) } for t in minute['tweets'] ]
        # pass list to jian to get avg sentiment
        tweets_with_sentiment.append([{ 'minute': minute['_id'], 'avgSentiment': avg_sentiment, 'tweets': tweet_sentiments }])

    # match_tweets['results']
    return tweets_with_sentiment

# Mongo shell format looks like this:  ISODate("2014-06-12T15:31:18Z")
# get the substring from (0,17) for minutes
# db.tweets.find({'timestamp': { $gte: ISODate("2014-06-12T15:31:18Z") }} ).count()
# This query works in the mongo shell:

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

    getTimeDump(start, end, dump_location)
