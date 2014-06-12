#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function,division
from argparse import ArgumentParser

from twitterstream import TwitterClient
from mongo_client import MongoWriter


# open the twitterstream
# crawl tweets and pop them into mongo

def crawl_tweets(config):
    mClient = MongoWriter('wcTweets', 'tweets')
    tClient = TwitterClient(config)

    # begin fetching, and storing to mongo
    tClient.begin_fetching(mClient.save_data)

if __name__ == '__main__':
    # let user give a location to put tweets into
    parser = ArgumentParser()
    parser.add_argument("configuration_file", action="store", help="path to the config file (in YAML format).")
    args = parser.parse_args()

    cfg_path = args.configuration_file
    crawl_tweets(cfg_path)