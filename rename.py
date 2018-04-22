from pymongo import MongoClient

# https://jira.mongodb.org/browse/TOOLS-1163
# MONGODB SUCKS

if __name__ == '__main__':

    client = MongoClient()
    db = client.research
    collections = db.collection_names()

    for collection in collections:
        if '/' in collection:
            new_name = collection.replace('/', ' ')
            print collection, '-->', new_name
            db[collection].rename(new_name)