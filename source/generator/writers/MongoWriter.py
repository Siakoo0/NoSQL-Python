from ..entities.Entity import Entity

from bson.objectid import ObjectId
from concurrent.futures import ThreadPoolExecutor

from source.database.MongoDB import MongoDB
from source.tools.Logger import Logger

class MongoWriter:
    def __convert(self, entity: Entity, collectionMap):
        document = entity.getAttributes().copy()
        
        if entity.hasMeta("MongoID") :
            document["_id"] = entity.getMeta("MongoID") 
        else:
            document["_id"] = ObjectId()

        relationships = entity.getRelationships()

        for entityRelName in relationships.keys():
            eRelated = relationships[entityRelName]

            if not hasattr(eRelated, "__len__"):
                if eRelated.hasMeta("collection"):
                    document[eRelated.getMeta("type").lower()] = eRelated.getMeta("MongoID") if eRelated.hasMeta("MongoID") else self.save(eRelated, collectionMap)["_id"]
                else:
                    convertedDoc = self.__convert(eRelated, collectionMap)
                    
                    if isinstance(convertedDoc, dict) and len(convertedDoc.keys()) < 3:
                        document[eRelated.getMeta("type").lower()] = convertedDoc["value"]
                    else:
                        document[eRelated.getMeta("type").lower()] = convertedDoc
            else:
                relatedEntities = []
                for eRel in eRelated:
                    if eRel.hasMeta("collection"):
                        documentTemp = eRel.getMeta("MongoID") if eRel.hasMeta("MongoID") else self.save(eRel)["_id"]
                    else:
                        documentTemp = self.__convert(eRel, collectionMap)
                        
                        if isinstance(convertedDoc, dict) and len(convertedDoc.keys()) < 3:
                            document[eRel.getMeta("type").lower()] = convertedDoc["value"]
                        else:
                            document[eRel.getMeta("type").lower()] = convertedDoc
                    
                    relatedEntities.append(documentTemp)

                document[eRelated.getMeta("type").lower()] = relatedEntities

        return document

    def save(self, entity: Entity, collectionMap : dict):
        if not entity.hasMeta("collection"):
            return

        document = self.__convert(entity, collectionMap)

        entity.setMeta("MongoID", document["_id"])
        
        if not entity.getMeta("collection") in collectionMap.keys():
            collectionMap[entity.getMeta("collection")] = []
            
        collectionMap[entity.getMeta("collection")].append(document)
        
        return document
        
    def divide_chunks(self, l, n):
            for i in range(0, len(l), n):
                yield l[i:i + n]    
    
    def load(self, name, collection):
        with ThreadPoolExecutor(10) as pool: 
            for chunk in collection:
                pool.submit(self.loadToMongo, name, chunk)
                     
    def loadToMongo(self, name, chunk):
        MongoDB.db.get_collection(name).insert_many(chunk, bypass_document_validation=True)
        
    def write(self, entitiesDict, percentage, totalEntities):
        collections = {}
        
        with ThreadPoolExecutor(10) as pool:
            for entsList in entitiesDict.values():
                for entity in entsList:
                    pool.submit(self.save, entity, collections)
            
        with ThreadPoolExecutor(len(collections.keys())) as pool:
            for key, collection in collections.items():
                self.load(key, list(self.divide_chunks(collection, 1000)))
            
        Logger.log(f"[ Dataset {totalEntities} ~ Mongo Database] Caricamento dati perc. {percentage}% effettuato con successo.")
