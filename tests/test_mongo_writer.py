#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import json as json

# todo: the module name 'twitter' conflicts with the python twitter module
import twitterstream
from mongo_client import MongoWriter
import ipdb as ipdb

'''
@author: Chris Hokamp
@contact: chris.hokamp@gmail.com
@license: WTFPL
'''

sample_tweet='{"contributors": null, "truncated": false, "text": "RT @worldsoccershop: ONLY 1 MORE DAY UNTIL THE #WORLDCUP! http://t.co/o1ZxfonOLM"}'

class TestMongoWriter(unittest.TestCase):

    # setup -- create a rf_tagger, morph_converter, and IndexMapper, and test that they work together
    def setUp(self):
        self.db = MongoWriter('test_tweets', 'tweets')

    def test_adding_tweets(self):
        tweet_dict = json.loads(sample_tweet)
        item_id = self.db.insert_data(tweet_dict)
        retrieved_tweet = self.db.collection.find_one({u'_id': item_id})
        print(retrieved_tweet)
        self.assertTrue(retrieved_tweet is not None)

if __name__ == '__main__':
    unittest.main()
