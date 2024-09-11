
from pymongo import MongoClient
from bson.objectid import ObjectId

class DbService:

    def __init__(self, **params) -> None:
        self.mongo = MongoCRUD(params['conn_str'], params['dbname'])

    def get_collection(self, collectionName, createIfNotExists=True):
        if not collectionName in self.mongo.db.list_collection_names():
            if createIfNotExists:
                print("creating new collection: " + collectionName)
                self.mongo.db.create_collection(collectionName)
            else:
                return None
        return self.mongo.db[collectionName]

    def put_jobcz_details(self, **record):
        job = self.mongo.get_by_field('jobcz_details', 'job_id', record['job_id'])
        if not job:
            self.mongo.create('jobcz_details', record)
        else:
            self.mongo.update('jobcz_details', {'job_id': record['job_id']}, {'job_offer': record['job_offer']})

    def put_all_jobcz_details(self, records):
        self.mongo.create('jobcz_details', records, ismany=True)

    def get_jobcz_details(self):
        return self.mongo.read('jobcz_details', {'job_offer': {'$exists': True}})

    def put_jobcz_details_embedding(self, obj):
        self.mongo.update('jobcz_details_embedded', query={'_id': obj['_id']}, update_values=obj, isReplace=True)

    def delete_field(self, collectionName, fieldName):
        self.mongo.update(collectionName, {}, fieldname=fieldName)

    def vector_search(self, collectionName, queryVector, path, index, numCandidate=100, limit=4):
        return self.mongo.db[collectionName].aggregate([
            {
                "$vectorSearch": {
                    "queryVector": queryVector,
                    "path": path,
                    "numCandidates": numCandidate,
                    "limit": limit,
                    "index": index,
                }
            }
        ])

class MongoCRUD:
    def __init__(self, connection_string, db):
        # Initialize MongoDB client
        self.client = MongoClient(connection_string)
        self.db = self.client[db]

    def is_collection_exists(self, collectionName, createIfNotExists=True):
        if not collectionName in self.db.list_collection_names():
            if createIfNotExists:
                print("creating new collection: " + collectionName)
                self.db.create_collection(collectionName)
            else:
                return False
        return True


    def create(self, collectionName, data, ismany=False):
        # Insert a document into the collection
        if self.is_collection_exists(collectionName):
            if not ismany:
                inserted_id = self.db[collectionName].insert_one(data).inserted_id
                return str(inserted_id)
            else:
                self.db[collectionName].insert_many(data)
        
    def read(self, collectionName, query=None):
        if query is None:
            query = {}
        if not self.is_collection_exists(collectionName):
            return []
        return list(self.db[collectionName].find(query))

    def update(self, collectionName, query, update_values=None, fieldname=None, isReplace=False):
        if not self.is_collection_exists(collectionName):
            return 0

        if isReplace:
            result = self.db[collectionName].replace_one(query, update_values)
        elif fieldname:
            result = self.db[collectionName].update_many(query, { "$unset": { "field_to_remove": fieldname } })
        else:
            result = self.db[collectionName].update_many(query, {'$set': update_values})
        return result.modified_count

    def delete(self, collectionName, query):
        if not self.is_collection_exists(collectionName):
            return 0

        result = self.db[collectionName].delete_many(query)
        return result.deleted_count

    def get_by_field(self, collectionName, field, val):
        if not self.is_collection_exists(collectionName):
            return None

        return self.db[collectionName].find_one({f'{field}': val})

    def aggregate(self, collectionName, query):
        return self.db[collectionName].aggregate(query)

    def close_connection(self):
        # Close the MongoDB connection
        self.client.close()