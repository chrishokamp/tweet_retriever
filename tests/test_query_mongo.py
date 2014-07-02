#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division
import unittest
import json as json
import datetime
import pymongo

import query_mongo


import ipdb as ipdb

'''
@author: Chris Hokamp
@contact: chris.hokamp@gmail.com
@license: WTFPL
'''

class TestQueryMongo(unittest.TestCase):

    # def setUp(self):

    # @unittest.skip('skipping')
    def test_tweet_aggregation(self):
        aggregated_tweets = query_mongo.create_match_with_sentiment({})
        print(json.dumps(aggregated_tweets))
        print("Number of Groups: {}".format(str(len(aggregated_tweets['result']))))

if __name__ == '__main__':
    unittest.main()