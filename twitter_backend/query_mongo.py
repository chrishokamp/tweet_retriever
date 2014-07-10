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
import re, math
from tweet_sentiment import ruleBased

from convert_timestamp import convert_timestamp
import pymongo
from pymongo.errors import OperationFailure

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
        return True
    else:
        return False

#find team name from a tweet, return true if find, else false
def exclusive_contains_entity(name, tweet, dont_include=[]):
    exclusive_match = True
    for w in dont_include:
        if re.search( r'\b' + re.escape(w) + r'\b', tweet, re.M|re.I):
            exclusive_match=False
    if exclusive_match and re.search( r'\b' + re.escape(name) + r'\b', tweet, re.M|re.I):
        return True
    else:
        return False

# assume that minute_list is already sorted by time, ascending
def get_sentiment_by_minute(minute_list, entity_list):
    entity_sentiments = defaultdict(list)
    for minute in minute_list:
        minute_tweets = minute['tweets']
        # filter tweets by entity mention
        for idx,entity in enumerate(entity_list):
            # entity_tweets = [ t for t in minute_tweets if contains_entity(entity, t['text']) ]
            other_entities=list(entity_list)
            del other_entities[idx]
            entity_tweets = [ t for t in minute_tweets if exclusive_contains_entity(entity, t['text'], dont_include=other_entities) ]
            print(entity)
            # avoid division by 0
            num_tweets = 1
            if len(entity_tweets) > 0:
                num_tweets = len(entity_tweets)
            avg_sentiment = sum([get_sentiment(t['text']) for t in entity_tweets ]) / num_tweets
            # IMPORTANT: add 'Z' to make this UTC! (ISODate format)
            entity_sentiments[entity].append( { 'timestamp': minute['_id']+'Z', 'sentiment': avg_sentiment } )

    tweets_with_sentiment = {}
    tweets_with_sentiment['entitySentiments'] = []
    for entity, sentiment_list in entity_sentiments.items():
        tweets_with_sentiment['entitySentiments'].append({ 'entityName': entity, 'sentiments': sentiment_list })

    return tweets_with_sentiment

# match_obj is { 'matchName', 'time': { start_time: '', end_time: '' }} --> times are in the format: 'Thu Jun 12 16:08:42 +0000 2014'
# Mongo shell format looks like this:  ISODate("2014-06-12T15:31:18Z")
# get the substring from (0,17) for minutes
# db.tweets.find({'timestamp': { $gte: ISODate("2014-06-12T15:31:18Z") }} ).count()
def get_tweets_in_window(match_obj, batches=False):
    match_name = match_obj['matchName']
    entity_list = match_obj['entities']
    significant_events = match_obj['significantEvents']
    start_time = convert_timestamp(match_obj['startTime'])
    end_time = convert_timestamp(match_obj['endTime'])

    minute_groups = []
    # aggregate in batches if the data is big
    if batches:
        # separate the time window into N batches
        # end_time - start_time
        diff = end_time - start_time
        total_minutes, remainder = divmod(diff.seconds, 60)
        # add one to cover the remainder
        total_minutes += 1
        # 10min batches
        num_batches = int(math.ceil(total_minutes / 10))
        batch_size = datetime.timedelta(minutes=10)


        for i in range(num_batches):
            batch_start = start_time + (i * batch_size)
            batch_end = start_time + batch_size
            match_tweets = collection.aggregate([{'$match': { 'timestamp': { '$gte': start_time, '$lte': end_time }}},
                                                      {'$project': { 'minute': {'$substr': ['$timestamp', 0, 16]}, 'text': 1 }},
                                                      {'$group': { '_id': '$minute', 'tweets': { '$push': { 'text': '$text' }}}}])
            minute_groups += sorted(match_tweets['result'], key=lambda x: x['_id'])

    else:
        try:
            match_tweets = collection.aggregate([{'$match': { 'timestamp': { '$gte': start_time, '$lte': end_time }}},
                                                      {'$project': { 'minute': {'$substr': ['$timestamp', 0, 16]}, 'text': 1 }},
                                                      {'$group': { '_id': '$minute', 'tweets': { '$push': { 'text': '$text' }}}}])

            # sort by minute
            minute_groups = sorted(match_tweets['result'], key=lambda x: x['_id'])
        except OperationFailure:
            pass

    tweets_with_sentiment = get_sentiment_by_minute(minute_groups, entity_list)
    tweets_with_sentiment['matchName'] = match_name
    tweets_with_sentiment['startTime'] = start_time
    tweets_with_sentiment['endTime'] = end_time
    tweets_with_sentiment['significantEvents'] = significant_events

    return tweets_with_sentiment

# Working - add function to compute by time interval (start with 10 min)
#     yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)


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
