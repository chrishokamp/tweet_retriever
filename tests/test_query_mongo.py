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

import ipdb as ipdb


class TestQueryMongo(unittest.TestCase):

    # def setUp(self):

    # @unittest.skip('skipping')
    def test_tweet_aggregation(self):
        # match_obj is { 'matchName', 'time': { start_time: '', end_time: '' }} --> times are in the format: 'Thu Jun 12 16:08:42 +0000 2014'

        start_time = 'Wed Jul 2 14:00:00 +0000 2014'
        end_time = 'Wed Jul 2 15:00:00 +0000 2014'
        entity_list = ['germany', 'ghana']
        match_obj={ 'matchName': 'germany_ghana', 'time': { 'startTime': start_time, 'endTime': end_time }, 'entities': entity_list}
        aggregated_tweets = query_mongo.get_tweets_in_window(match_obj)
        # aggregated_tweets = aggregated_tweets[:10]
        print(json.dumps(aggregated_tweets))
        # print("Number of Groups: {}".format(str(len(aggregated_tweets))))

if __name__ == '__main__':
    unittest.main()