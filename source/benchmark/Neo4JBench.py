import time

from source.database.Neo4J import Neo4J
from source.tools.ResultWriter import ResultWriter
from source.tools.Logger import Logger

from datetime import datetime, timedelta

from random import randint

cronometer = lambda : int(round(time.time() * 100))

class Neo4JBench:
    def __init__(self, dimension):
        self.writer = ResultWriter(dimension, "Neo4J")

    def benchmark(self, percentage):
        connection = Neo4J.driver
        formatDate = '%Y-%m-%dT%H:%M:%S.%f%z'
        
        results = []
        
        Neo4J.clearCache()
        
        dateStart = datetime.now() - timedelta(days=45)
        dateStart = dateStart.strftime(formatDate)
        dateEnd = datetime.now() + timedelta(days=45)
        dateEnd = dateEnd.strftime(formatDate)
        
        queries = [
            self.firstQuery(), 
            self.secondQuery(), 
            self.thirdQuery(), 
            self.fourthQuery(dateStart, dateEnd)
        ]
        
        Logger.log(f"Inizio benchmark Neo4J percentuale {percentage}%.")
        
        for _ in range(31):
            resultRow = []
            
            for query in queries:
                with connection.session() as session:
                    start = cronometer()
                    session.run(query)
                    resultRow.append(cronometer() - start)
                    
            results.append(resultRow)

        self.writer.writeResults(str(percentage), results)
        
        Logger.log(f"Scrittura risultati Neo4J percentuale {percentage}%.")

    def firstQuery(self) -> str:
        return   """MATCH (n:Claim)
                    WHERE n.closed_date IS NOT NULL
                    RETURN *"""
            
    def secondQuery(self) -> str:
        return  """MATCH (customer:Customer)-[:OPEN]-(claim:Claim)
                   WHERE claim.closed_date IS NULL
                   RETURN *"""
        
    def thirdQuery(self) -> str:
        return """MATCH (a:Customer)-[:HAS_PHONE|HAS_SSN|HAS_EMAIL]-(m)-[:HAS_PHONE|HAS_SSN|HAS_EMAIL]-(b:Customer) 
                  RETURN *"""

    
    def fourthQuery(self, dateStart, dateEnd) -> str:
        return  f"""MATCH (c:Customer)-[:OPEN]-(claim:Claim)
                    MATCH (claim)-[:DEALS_WITH]-(l:Lawyer)
                    MATCH (claim)-[:CHECKS]-(e:Evaluator)
                    WHERE claim.closed_date IS NULL AND 
                    claim.open_date > DATETIME('{dateStart}') 
                    AND claim.open_date < DATETIME('{dateEnd}')
                    RETURN *"""
                  
    def save(self):
        Logger.log(f"Salvataggio risultati Neo4J.")
        self.writer.save()
