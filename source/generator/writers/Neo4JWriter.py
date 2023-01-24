from ..entities.Entity import Entity
from datetime import datetime

from source.tools.CSV import CSV
from source.tools.Logger import Logger
from source.database.Neo4J import Neo4J

from concurrent.futures import ThreadPoolExecutor
from time import sleep

import docker

class Neo4JWriter:
    __uniqueID = 0
    
    @staticmethod
    def getUniqueID():
        value = Neo4JWriter.__uniqueID
        Neo4JWriter.__uniqueID +=1
        return value
    
    @staticmethod
    def convert(entity : Entity):
        attributes = entity.getAttributes()
        
        for name, attribute in attributes.items():
            if isinstance(attribute, datetime):
                attributes[name] = attribute.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
        
        uniqueID = Neo4JWriter.getUniqueID()
        
        entity.setMeta("neo4J_ID", uniqueID)
        
        return {":ID" : uniqueID, ":LABEL": entity.getMeta("type")} | attributes
    
    @staticmethod
    def writeCSV(name, path, data):
        CSV(name).write(path, data)
    
    @staticmethod
    def writeRelations(relationships, paths, fileNames : list):
        with ThreadPoolExecutor(10) as pool:
            for _, relationship in relationships.items():
                if not "_".join(relationship["nodes"]) in fileNames:
                    fileNames.append("_".join(relationship["nodes"]))
                    
                for path in paths:
                    pool.submit(Neo4JWriter.writeCSV, "_".join(relationship["nodes"]), path, relationship["content"])

    @staticmethod
    def writeEntities(entities, paths, fileNames):
        with ThreadPoolExecutor(10) as pool:
            for entityName, entitiesList in entities.items():
                if not "_".join(entityName) in fileNames:
                    fileNames.append(entityName)
                    
                for path in paths:
                    pool.submit(Neo4JWriter.writeCSV, entityName, path, entitiesList)

                    
    @staticmethod
    def write(entitiesDict, percentage : int, dimension : int):
        entities = {}
        relationships = {}
        
        for entsList in entitiesDict.values():
            for entitiesList in entsList:
                typeEnt = entitiesList.getMeta("type")
                if typeEnt not in entities.keys() : entities[typeEnt] = []
                
                entities[typeEnt].append(Neo4JWriter.convert(entitiesList))
                
                for nameRelation, relatedEntity in entitiesList.getRelationships().items():
                    typeRelatedEnt = relatedEntity.getMeta("type")
                    
                    
                    if not relatedEntity.hasMeta("neo4J_ID"):
                        entConverted = Neo4JWriter.convert(relatedEntity)
                        if typeRelatedEnt not in entities.keys() : entities[typeRelatedEnt] = []
                        
                        entities[typeRelatedEnt].append(entConverted)
                    
                    if not nameRelation in relationships.keys():
                        relationships[nameRelation] = {
                            "nodes" : [typeEnt, typeRelatedEnt], 
                            "content": []
                        }
                                        
                    if entitiesList.hasMeta("reverse_relations"):
                        relation = {
                            ':START_ID' : relatedEntity.getMeta("neo4J_ID"), 
                            ':END_ID' : entitiesList.getMeta("neo4J_ID"),
                            ':TYPE' : nameRelation
                        }
                    else:
                        relation = {
                            ':START_ID' : entitiesList.getMeta("neo4J_ID"),
                            ':END_ID' : relatedEntity.getMeta("neo4J_ID"), 
                            ':TYPE' : nameRelation
                        }
                        
                    relationships[nameRelation]["content"].append(relation)
                    
        paths = [
            "../.neo4j/import/",
            f"data/{dimension}/dataset/{percentage}/"
        ]
        
        entitiesFilenames = []
        relationsFilenames = []
        
        with ThreadPoolExecutor(2) as pool:
            pool.submit(Neo4JWriter.writeEntities, entities, paths, entitiesFilenames)
            pool.submit(Neo4JWriter.writeRelations, relationships, paths, relationsFilenames)
            
        Neo4JWriter.load(entitiesFilenames, relationsFilenames)
        
        Logger.log(f"[ Dataset {dimension} ~ Neo4J Database] Caricamento dati perc. {percentage}% effettuato con successo.")
        
        
    @staticmethod    
    def load(entitiesFilenames, relationsFilenames):
        client = docker.from_env()
        container = client.containers.get("neo4j_new")
        
        relationships = " ".join([f"--relationships  /import/{filename}.csv " for filename in relationsFilenames])
        
        nodes = " ".join([f"--nodes /import/{filename}.csv " for filename in entitiesFilenames])
        
        commands = [
            "neo4j stop",
            f"neo4j-admin database import full --delimiter=\";\" --array-delimiter=\"|\" --overwrite-destination {nodes} {relationships}",
            "neo4j start"
        ]
        
        for command in commands:
            container.exec_run(command)
            
        sleep(10)
        
        Neo4J.resetConnection()