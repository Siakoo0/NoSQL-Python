from .MongoWriter import MongoWriter
from .Neo4JWriter import Neo4JWriter

from source.database.Database import Database

def write(entitiesDict : dict, dimension : int, percentage : int):
    writers = [
        MongoWriter(), 
        Neo4JWriter()
    ]
        
    Database.emptyDatabase()
    
    for writer in writers:
        writer.write(entitiesDict, dimension, percentage)