#!/usr/bin/env python
#encoding: utf-8

'''
@author: Chris Hokamp
@contact: chris.hokamp@gmail.com
@license: WTFPL
'''

import unittest
from datetime import datetime
from convert_timestamp import convert_timestamp

class TestDatetimeConversion(unittest.TestCase):
    def test_tweet_client(self):
        created_at = 'Mon Jun 8 10:51:32 +0000 2009'
        dt = convert_timestamp(created_at)
        ref_dt = datetime(2009, 6, 8, 10, 51, 32)
        self.assertEqual(dt, ref_dt)


if __name__ == '__main__':
    unittest.main()
