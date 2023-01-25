from ..entities.Entity import Entity
from datetime import datetime

from source.tools.CSV import CSV
from source.tools.Logger import Logger
from source.database.Neo4J import Neo4J

from concurrent.futures import ThreadPoolExecutor
from time import sleep
import os


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
        
        dateTimeChg = []
        
        for name, attribute in attributes.items():
            if isinstance(attribute, datetime):
                attributes[name] = attribute.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
                dateTimeChg.append(name)
        
        for name in dateTimeChg:
            attributes[name + ":datetime"] = attributes[name]
            del attributes[name]
        
        uniqueID = Neo4JWriter.getUniqueID()
        
        entity.setMeta("neo4J_ID", uniqueID)
        
        return {":ID" : uniqueID, ":LABEL": entity.getMeta("type")} | attributes
    
    @staticmethod
    def writeCSV(name, path, data):
        CSV(name).write(path, data)
    
    @staticmethod
    def writeRelations(relationships, paths):
        with ThreadPoolExecutor(10) as pool:
            for _, relationship in relationships.items():                    
                for path in paths:
                    pool.submit(Neo4JWriter.writeCSV, "_".join(relationship["nodes"]), path, relationship["content"])

    @staticmethod
    def writeEntities(entities, paths):
        with ThreadPoolExecutor(10) as pool:
            for entityName, entitiesList in entities.items():
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
        
        with ThreadPoolExecutor(2) as pool:
            pool.submit(Neo4JWriter.writeEntities, entities, paths)
            pool.submit(Neo4JWriter.writeRelations, relationships, paths)
            
        Neo4JWriter.load()
        
        Logger.log(f"[ Dataset {dimension} ~ Neo4J Database] Caricamento dati perc. {percentage}% effettuato con successo.")
        
        
    @staticmethod    
    def load():
        client = docker.from_env()
        container = client.containers.get("neo4j_new")
        
        relationships = []
        nodes = []
        
        for filename in os.listdir("../.neo4j/import"):
            if "_" in filename:
                relationships.append(f"--relationships  /import/{filename} ")
            else:
                nodes.append(f"--nodes /import/{filename} ")
                
                
        relationships = " ".join(relationships)
        nodes = " ".join(nodes)
        
        commands = [
            "neo4j stop",
            f"neo4j-admin database import full --delimiter=\";\" --array-delimiter=\"|\" --overwrite-destination  --skip-duplicate-nodes {nodes} {relationships}",
            "neo4j start"
        ]
        
        for command in commands:
            container.exec_run(command)
            
        sleep(20)
        
        Neo4J.resetConnection()
        

# neo4j-admin database import full --delimiter=";" --array-delimiter="|" --overwrite-destination \
# --nodes /import/Address.csv  \
# --nodes /import/Claim$1.csv  \
# --nodes /import/Claim.csv \
# --nodes /import/Customer.csv  \
# --nodes /import/Email.csv \
# --nodes /import/Evaluator.csv  \
# --nodes /import/Lawyer.csv  \
# --nodes /import/Phone.csv  \
# --nodes /import/SSN.csv  \
# --relationships  /import/Claim_Customer.csv  \
# --relationships  /import/Claim_Evaluator.csv  \
# --relationships  /import/Claim_Lawyer.csv  \
# --relationships  /import/Customer_Address.csv  \
# --relationships  /import/Customer_Email.csv  \
# --relationships  /import/Customer_Phone.csv \
# --relationships  /import/Customer_SSN.csv