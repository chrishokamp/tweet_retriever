#!/usr/bin/env python
#encoding: utf-8

'''
@author: Chris Hokamp
@contact: chris.hokamp@gmail.com
@license: WTFPL
'''

from datetime import datetime

# created_at = 'Mon Jun 8 10:51:32 +0000 2009' # Get this string from the Twitter API
def convert_timestamp(twitter_timestamp):
    dt = datetime.strptime(twitter_timestamp, '%a %b %d %H:%M:%S +0000 %Y')
    return dt