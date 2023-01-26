from neo4j import GraphDatabase

class Neo4J:
    uri, user, password = "neo4j://localhost:7687", "neo4j", "siakoo1234"
    driver = GraphDatabase.driver(uri, auth=(user, password))

    @staticmethod
    def resetConnection():
        Neo4J.driver = GraphDatabase.driver(Neo4J.uri, auth=(Neo4J.user, Neo4J.password))
    
    @staticmethod
    def executeQuery(query, resultBoolean = True):
        with Neo4J.driver.session() as s:
            res = s.run(query=query)
            if resultBoolean:
                return list(res)
    
    @staticmethod
    def empty():
        Neo4J.executeQuery("MATCH (n) DETACH DELETE n", False)
    
    @staticmethod
    def clearCache():
        Neo4J.executeQuery("CALL db.clearQueryCaches();", False)