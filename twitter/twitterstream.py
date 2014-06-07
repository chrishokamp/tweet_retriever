#!/usr/bin/env python
#encoding: utf-8

'''
@author: Chris Hokamp
@contact: chris.hokamp@gmail.com
@license: WTFPL
'''

from __future__ import print_function,division
from argparse import ArgumentParser
import oauth2 as oauth
import urllib2 as urllib
import yaml, json



signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

_debug = 0
http_handler = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

oauth_token=''
oauth_token=''

def twitterreq(url, method, parameters):
    req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=method,
                                             http_url=url,
                                             parameters=parameters)

    req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

    # headers = req.to_header()

    if method == "POST":
        encoded_post_data = req.to_postdata()
    else:
        encoded_post_data = None
        url = req.to_url()

    opener = urllib.OpenerDirector()
    opener.add_handler(http_handler)
    opener.add_handler(https_handler)

    response = opener.open(url, encoded_post_data)
    return response

def fetchsamples(parameters, filters):
    url = "https://stream.twitter.com/1.1/statuses/filter.json"

    response = twitterreq(url, "POST", parameters)
    for line in response:
        tweet = json.loads(line.decode('utf8'))
        if filters is not None > 0:
            tweet = { key: tweet[key] for key in tweet.keys() if key in filters }
        print(json.dumps(tweet))

if __name__ == '__main__':

    # get the command line options
    parser = ArgumentParser()
    parser.add_argument("configuration_file", action="store", help="path to the config file (in YAML format).")
    args = parser.parse_args()

    cfg_path = args.configuration_file

    config = None
    parameters = {}
    filters = {}
    credentials = {}

    # read configuration file
    with open(cfg_path, "r") as cfg_file:
        config = yaml.load(cfg_file.read())
        parameters = config.get('parameters', {})
        filters = config.get('filters', {})
        credentials = config.get('credentials', {})

    access_token_key = credentials['access_token_key']
    access_token_secret = credentials['access_token_secret']
    api_consumer_key = credentials['api_consumer_key']
    api_consumer_secret = credentials['api_consumer_secret']

    oauth_token = oauth.Token(key=access_token_key, secret=access_token_secret)
    oauth_consumer = oauth.Consumer(key=api_consumer_key, secret=api_consumer_secret)
    fetchsamples(parameters, filters)

    #TEST
    #http://search.twitter.com/search.json?q=%23occupytaksim
