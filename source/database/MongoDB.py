from pymongo import MongoClient

class MongoDB:
    client = MongoClient("mongodb://root:example@localhost:27017")
    db = client["nosql"]
    
    @staticmethod
    def empty():
        for collection in MongoDB.db.list_collection_names():
            MongoDB.db.get_collection(collection).delete_many({})
        
    
    @staticmethod
    def clearCache():
        for collection in MongoDB.db.list_collection_names():
            MongoDB.db.command({"planCacheClear": collection})