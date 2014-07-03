#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@author: Chris Hokamp
@contact: chris.hokamp@gmail.com
@license: WTFPL
'''

from __future__ import print_function,division
import datetime
from bson import json_util
import os, yaml, codecs, random, json
from argparse import ArgumentParser
from collections import defaultdict
import re
from tweet_sentiment import ruleBased

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
    return ruleBased.calculateScore(text)

#find team name from a tweet, return true if find, else false
def contains_entity(name, tweet):
    if re.search( r'\b' + re.escape(name) + r'\b', tweet, re.M|re.I):
        return True;
    else:
        return False;
# def contains_entity(entity, text):
#     return True

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
    entity_list = match_obj['entities']
    start_time = convert_timestamp(match_obj['time']['startTime'])
    end_time = convert_timestamp(match_obj['time']['endTime'])

    # $match, $project the minute substring from the timestamp, then $group by minute
    match_tweets = collection.aggregate([{'$match': { 'timestamp': { '$gte': start_time, '$lte': end_time }}},
                                              {'$project': { 'minute': {'$substr': ['$timestamp', 0, 16]}, 'text': 1 }},
                                              {'$group': { '_id': '$minute', 'tweets': { '$push': { 'text': '$text' }}}}])

    tweets_with_sentiment = {}
    entity_sentiments = defaultdict(list)
    for minute in match_tweets['result']:
        minute_tweets = minute['tweets']
        # filter tweets by entity mention
        for entity in entity_list:
            entity_tweets = [ t for t in minute_tweets if contains_entity(entity, t['text']) ]
            print(entity)
            # avoid division by 0
            num_tweets = 1
            if len(entity_tweets) > 0:
                # using the total number of tweets instead of entity tweets as a proxy to 'current hype about this entity'
                num_tweets = len(entity_tweets)
            avg_sentiment = sum([get_sentiment(t['text']) for t in entity_tweets ]) / num_tweets
            entity_sentiments[entity].append(avg_sentiment)
    tweets_with_sentiment['entitySentiments'] = []
    for entity, sentiment_list in entity_sentiments.items():
        tweets_with_sentiment['entitySentiments'].append({ 'entityName': entity, 'sentiments': sentiment_list })

        # tweet_sentiments = [ { 'text': t['text'], 'sentiment': get_sentiment(t['text']) } for t in minute['tweets'] ]
        # pass list to jian to get avg sentiment
        # tweets_with_sentiment.append([{ 'minute': minute['_id'], 'avgSentiment': avg_sentiment, 'tweets': tweet_sentiments }])

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
