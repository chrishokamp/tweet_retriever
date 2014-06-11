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



class TestTwitterStream(unittest.TestCase):

    # setup -- create a rf_tagger, morph_converter, and IndexMapper, and test that they work together
    def setUp(self):
        config = '/home/chris/projects/twitter/twitter_interface/tweet_retriever/config/chris.yaml'
        self.client = twitterstream.TwitterClient(config)
        self.db = MongoWriter('test_tweets', 'tweets')


    def test_tweet_client(self):
        self.assertTrue(self.client.parameters is not None, 'the client should have some parameters for filtering')

    # @unittest.skip("not implemented")
    def test_tweet_streaming(self):
        def mock_storage_function(item):
            print("STORING ITEM: " + json.dumps(item))
        self.client.begin_fetching(store_func=mock_storage_function)
        self.assertTrue(len(self.tweet_storage) > 0)


if __name__ == '__main__':
    unittest.main()

