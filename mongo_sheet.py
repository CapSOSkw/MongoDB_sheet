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
    # 1st way
    conn = MongoClient(host=account.host,
                       port=account.port,
                       username=account.username,
                       password=account.password,
                       authSource=account.authSource)
    # conn = MongonClient(host='127.0.0.1', port=27017)  # for connect with localhost

    # 2nd way
    mongo_uri = "mongodb://{0}:{1}@{2}:{3}/{4}".format(account.username, account.password,
                                                     account.host, account.port, account.authSource)
    mongo_uri1 = f'mongodb://{account.username}:{account.password}@{account.host}:{account.port}/{account.authSource}'

    # mongo_uri = 'mongodb://127.0.0.1:27017'  # for connect with localhost

    connect = MongoClient(mongo_uri1)   # connect to mongodb
    db = connect.Your_database_name   # connect to database
    collection = db.Your_collection   # connect to collection


def insert_from_json(json_file):
    '''
    Load json file and insert it into database
    For more information, see
    https://docs.mongodb.com/manual/reference/method/db.collection.insert/
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
    https://docs.mongodb.com/manual/reference/method/db.collection.find/
    :return:
    '''
    data = connect_mongodb.collection.find_one({'Key': 'Value'})

    #  find_one with conditions
    data2 = connect_mongodb.collection.find_one({'$and': [
        {'Key1': 'Value1'},
        {'Key2': 'Value2'},
    ]})

    pprint(data)


def find_all_from_mongo():
    '''
    Used for finding all corresponding data in database.
    Then read or print data.
    For more query operators, see
    https://docs.mongodb.com/manual/reference/operator/query/
    https://docs.mongodb.com/manual/reference/method/db.collection.find/
    :return:
    '''
    data = connect_mongodb.collection.find({'Key': 'Value'})
    for i in data:
        pprint(i)


def update_data():
    '''
    Used for updating data.
    For more information, see
    https://docs.mongodb.com/manual/reference/method/db.collection.update/
    https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/
    :return:
    '''
    # Only used for updating for one data
    connect_mongodb.collection.update_one({
        'Key for locating': 'Value'
    }, {
        '$set': {
            'Key1 You want to update': 'New Value',
            'Key2 You want to update': 'New Value',
        }
    }, upsert=False)
    # Upsert If set to true, creates a new document when no document matches the query criteria.
    # The default value is false, which does not insert a new document when no match is found.


    # update for multiple data
    connect_mongodb.collection.update({
        'Key for locating': 'Value'
    }, {
        '$set': {
            'Key1 You want to update': 'New Value',
            'Key2 You want to update': 'New Value',
        }
    }, multi=True)
    # If set to true, updates multiple documents that meet the query criteria.
    # If set to false, updates one document. The default value is false. For additional information,


def remove_field():
    '''
    To remove the field from the document in the collection.
    For more information, see
    https://stackoverflow.com/questions/6851933/how-to-remove-a-field-completely-from-a-mongodb-document/6852039
    https://docs.mongodb.com/manual/reference/operator/update/unset/
    https://docs.mongodb.com/manual/tutorial/update-documents/#Updating-$unset
    :return:
    '''
    # remove from the first document it finds the matches
    connect_mongodb.collection.update({},
                                      {'$unset': {
                                          'Field You want to remove': 1
                                      }})
    # remove from all documents it finds the matches
    connect_mongodb.collection.update({},
                                      {'$unset': {
                                          'Field You want to remove': 1
                                      }}, multi=True)
    # Multi: If set to true, updates multiple documents that meet the query criteria.
    # If set to false, updates one document. The default value is false.


def delete_collection():
    '''
    To remove collection from database.
    For more information, see
    https://docs.mongodb.com/manual/reference/method/db.collection.deleteOne/
    https://docs.mongodb.com/manual/reference/method/db.collection.deleteMany/
    :return:
    '''
    # Specify an empty document {} to delete the first document returned in the collection.
    connect_mongodb.collection.delete_one({})

    # delete one collection based by its id
    connect_mongodb.collection.delete_one({'_id': ObjectId("ID VALUES")})

    # delete the first document with certain conditions.
    # For example, delete the first document which 'key' value is great than 1.
    connect_mongodb.collection.delete_one({'key': {'$gt': 1}})

    # delete all documents
    # To delete all documents in a collection, pass in an empty document ({}).
    connect_mongodb.collection.delete_many({})

    # delete all the documents if conditions match
    connect_mongodb.collection.delete_many({'key': {'$ls': 100}})
    connect_mongodb.collection.remove({'key': 'value'}, multi=True)  # multi default is True


def get_distinct():
    '''
    To get the distinct values from field.
    Finds the distinct values for a specified field across a single collection
    or view and returns the results in an array.
    For more information, see
    https://docs.mongodb.com/manual/reference/method/db.collection.distinct/
    :return:
    '''
    distinct_values = connect_mongodb.collection.distinct('Field name')
    pprint(distinct_values)


def drop_collection_from_db():
    '''
    Removes a collection or view from the database.
    The method also removes any indexes associated with the dropped collection.
    For more information, see
    https://docs.mongodb.com/manual/reference/method/db.collection.drop/
    :return:
    '''
    # Double check before dropping collections

    double_check = False
    if double_check == True:
        connect_mongodb.db.COLLECTION_TO_DROP.drop()


def get_all_indexes():
    '''
    Returns an array that holds a list of documents that identify and
    describe the existing indexes on the collection.
    For more information, see
    https://docs.mongodb.com/manual/reference/method/db.collection.getIndexes/#db.collection.getIndexes
    :return:
    '''
    for idx in connect_mongodb.db.COLLECTION.list_indexes():
        pprint(idx)


def drop_index():
    '''
    Drops or removes the specified index from a collection.
    For more information, see
    https://docs.mongodb.com/manual/reference/method/db.collection.dropIndex/
    :return:
    '''
    double_check = False
    if double_check == True:
        connect_mongodb.db.COLLECTION.drop_indexes('index name')


def rename_collection():
    '''
    Renames a collection.
    https://docs.mongodb.com/manual/reference/method/db.collection.renameCollection/
    :return:
    '''
    double_check = False
    if double_check == True:
        connect_mongodb.db.OLD_COLLECTION.rename("NEW COLLECTION NAME")

