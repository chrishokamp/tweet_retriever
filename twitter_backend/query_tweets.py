# connection = pymongo.Connection('mymongohostname.com')
# connection.my_database.my_collection.insert({
#     'created_at': dt,
#     # ... other info about the tweet ....
# }, safe=True)
#
# three_days_ago = datetime.datetime.utcnow() - datetime.timedelta(days=3)
# tweets = list(connection.my_database.my_collection.find({
#     'created_at': { '$gte': three_days_ago }
# }).sort([('created_at', pymongo.DESCENDING)]))