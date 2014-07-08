#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@author: Chris Hokamp
@contact: chris.hokamp@gmail.com
@license: WTFPL
'''

from __future__ import print_function, division
import unittest
import json as json
import datetime
import pymongo

import query_mongo
from convert_timestamp import convert_timestamp

# to let json know how to handle bson stuff
from bson import json_util

import os
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestQueryMongo(unittest.TestCase):

    # def setUp(self):

    @unittest.skip('skipping')
    def test_tweet_aggregation(self):
        # match_obj is { 'matchName', 'time': { start_time: '', end_time: '' }} --> times are in the format: 'Thu Jun 12 16:08:42 +0000 2014'

        start_time = 'Wed Jul 2 14:00:00 +0000 2014'
        end_time = 'Wed Jul 2 15:00:00 +0000 2014'
        entity_list = ['germany', 'ghana']
        match_obj={ 'matchName': 'germany_ghana', 'time': { 'startTime': start_time, 'endTime': end_time }, 'entities': entity_list}
        match_sentiment_data = query_mongo.get_tweets_in_window(match_obj)
        # aggregated_tweets = aggregated_tweets[:10]
        print(json.dumps(match_sentiment_data))
        # print("Number of Groups: {}".format(str(len(aggregated_tweets))))

    @unittest.skip('skipping')
    def test_building_match_collections(self):
        # match_obj is { 'matchName', 'time': { start_time: '', end_time: '' }} --> times are in the format: 'Thu Jun 12 16:08:42 +0000 2014'
        start_time = 'Wed Jul 2 14:00:00 +0000 2014'
        end_time = 'Wed Jul 2 15:00:00 +0000 2014'
        entity_list = ['germany', 'ghana']
        significant_events = [
            {
                "player": "V\u00edctor Bern\u00e1rdez",
                "team": "ghana",
                "type": "yellow card",
                "timestamp": "Wed Jul 2 14:21:00 +0000 2014"
            },
            {
                "player": "Carlo Costly",
                "team": "germany",
                "type": "goal",
                "timestamp": "Wed Jul 2 14:35:00 +0000 2014"
            },


            ]
        match_obj= { 'matchName': 'testMatch', 'time': { 'startTime': start_time, 'endTime': end_time }, 'entities': entity_list, 'significantEvents': significant_events }
        match_sentiment_data = query_mongo.get_tweets_in_window(match_obj)

        self.assertTrue(len(match_sentiment_data['entitySentiments'][0]['sentiments']) == len(match_sentiment_data['entitySentiments'][1]['sentiments']) == 61, 'the number of time windows in the data should be correct.')
        self.assertFalse(len(match_sentiment_data['entitySentiments'][0]['sentiments']) == len(match_sentiment_data['entitySentiments'][1]['sentiments']) == 60, 'the number of time windows in the data should not be wrong.')
        print(match_sentiment_data)
        with open('sample_match.out', 'w') as out:
            # out.write(json.dumps(match_sentiment_data, indent=4, object_hook=json_util.object_hook))
            out.write(json.dumps(match_sentiment_data, indent=4, default=json_util.default))
            # to use this format, do:\n",
            #json.loads(aJsonString, object_hook=json_util.object_hook)\n",

    def test_build_match_collection_from_file(self):
        # match_obj is { 'matchName', 'time': { start_time: '', end_time: '' }} --> times are in the format: 'Thu Jun 12 16:08:42 +0000 2014'
        with open(os.path.join(__location__, "../data/matches_and_events.pruned.json")) as matchfile:
            match_list=json.loads(matchfile.read())
            # print(json.dumps(match_list))
            # map each list entry into a match object for querying
            for match in match_list:
                # match_obj= { 'matchName': match['matchName'], 'time': { 'startTime': match['startTime'], 'endTime': match['endTime'] }, 'entities': match['entities'] }
                match_obj= match
                match_sentiment_data = query_mongo.get_tweets_in_window(match_obj)
                # check if the match data is empty (if we don't have data for this match)

                if len(match_sentiment_data['entitySentiments']) > 0:
                    # insert into matches table in mongo here
                    client = pymongo.MongoClient()
                    db = client['wcTweets']
                    collection = db['matches']
                    match_sentiment_data['match_name'] = match['matchName']
                    collection.insert(match_sentiment_data)

                    print(match_sentiment_data)
                    print('We have tweets for Match: {}'.format(match['matchName']))

if __name__ == '__main__':
    unittest.main()
