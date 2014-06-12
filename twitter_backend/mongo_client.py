#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@author: Chris Hokamp
@contact: chris.hokamp@gmail.com
@license: WTFPL
'''

from pymongo import MongoClient
from pymongo.errors import OperationFailure

class MongoWriter():
    def __init__(self, db_name, collection_name, db_location=None):
        client = MongoClient(db_location)
        db = client[db_name]
        self.collection = db[collection_name]

    def save_data(self, item):
        print('item is: ')
        print(item)
        item_id = self.collection.insert(item)
        return item_id

    def get_data(self, mongo_query):
        print(mongo_query)
        item = self.collection.find_one(mongo_query)
        return item
