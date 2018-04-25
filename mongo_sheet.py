from pymongo import MongoClient
from pprint import pprint
from bson.objectid import ObjectId
import json
from glob import iglob   # used for loading json files
import os
import urllib.parse   # used for deal with some special characters, such as "@"


class account(object):
    username = 'Your username'
    password = 'Your Password'
    host = 'Host address'
    port = 27017   # default port: 27017
    authSource = 'Your auth Database'  # defalut: admin


class connect_mongodb(object):
    '''
    way to connect mongodb and access to collections
    :return:
    '''
    # conn = MongoClient(host=account.host,
    #                    port=account.port,
    #                    username=account.username,
    #                    password=account.password,
    #                    authSource=account.authSource)
    #
    mongo_uri = "mongodb://{0}:{1}@{2}:{3}/{4}".format(account.username, account.password,
                                                     account.host, account.port, account.authSource)
    mongo_uri1 = f'mongodb://{account.username}:{account.password}@{account.host}:{account.port}/{account.authSource}'

    connect = MongoClient(mongo_uri1)   # connect to mongodb
    db = connect.Your_database_name   # connect to database
    collection = db.Your_collection   # connect to collection


def insert_from_json(json_file):
    '''
    Load json file and insert it into database
    :param json_file: json file name
    :return: insert into database
    '''
    with open(json_file) as f:
        # extract data from json
        json_data = json.load(f)

        # pprint(json_data)

        # insert data, also could use this command to insert python-dict data
        connect_mongodb.collection.insert(json_data)


def insert_many_json():
    '''
    Load all json files in a folder and insert each one into DB.
    You can use relative path or absolute path for the FOLDER in the argument.
    :return:
    '''
    for filename in iglob('./Folder/*.json'):
        with open(filename) as f:
            json_data = json.load(f)
            # json_data['_id'] = ObjectId()     # You can create ID, otherwise MongoDB will help you create
            # pprint(json_data)
            connect_mongodb.collection.insert(json_data)


def find_one_from_mongo():
    '''
    Commonly, used for searching for an unique key in database
    And you can use 'and' & 'or' to help search.
    For more query operators, see
    https://docs.mongodb.com/manual/reference/operator/query/
    :return:
    '''
    data = connect_mongodb.collection.find_one({'Key': 'Value'})

    # find_one with conditions
    data2 = connect_mongodb.collection.find_one({'$and': [
        {'Key1': 'Value1'},
        {'Key2': 'Value2'},
    ]})

    pprint(data)


def find_all_from_mongo():
    '''
    Used for finding all corresponding data in database.
    Then read or print data.
    :return:
    '''
    data = connect_mongodb.collection.find({'Key': 'Value'})
    for i in data:
        pprint(i)


### test
def test():
    pass








