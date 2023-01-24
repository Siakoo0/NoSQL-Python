from .MongoDB import MongoDB
from .Neo4J import Neo4J

class Database:
    databaseList = [
        MongoDB(),
        Neo4J()
    ]
    
    @staticmethod
    def emptyDatabase():
        for database in Database.databaseList:
            database.empty()
            
    @staticmethod
    def clearCache():
        for database in Database.databaseList:
            database.clearCache()