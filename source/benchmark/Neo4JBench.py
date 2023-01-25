import time

from source.database.Neo4J import Neo4J
from source.tools.ResultWriter import ResultWriter
from source.tools.Logger import Logger

from datetime import datetime, timedelta

cronometer = lambda : int(round(time.time() * 100))

class Neo4JBench:
    def __init__(self, dimension):
        self.writer = ResultWriter(dimension, "Neo4J")

    def benchmark(self, percentage):
        results = []
        
        formatDate = '%Y-%m-%dT%H:%M:%S.%f%z'

        dateStart = datetime.now() - timedelta(days=15)
        dateStart = dateStart.strftime(formatDate)
        dateEnd = datetime.now() + timedelta(days=15)
        dateEnd = dateEnd.strftime(formatDate)

        Neo4J.clearCache()

        
        queries = [
            self.firstQuery(), 
            self.secondQuery(), 
            self.thirdQuery(), 
            self.fourthQuery(dateStart, dateEnd)
        ]

        for _ in range(31):
            resultRow = []

            for query in queries:
                startTime = cronometer()                
                Neo4J.executeQuery(query, False)
                endTime = cronometer() - startTime
                
                resultRow.append(endTime)

            results.append(resultRow)

        self.writer.writeResults(str(percentage), results)
        
        Logger.log(f"Scrittura risultati Neo4J percentuale {percentage}%.")

    def firstQuery(self) -> str:
        return """MATCH (n:Claim)
                  WHERE n.closed_date IS NOT NULL
                  RETURN *"""

    def secondQuery(self) -> str:
        return """MATCH (customer:Customer)-[:OPEN]-(claim:Claim)
                  WHERE claim.closed_date IS NULL
                  RETURN *"""

    def thirdQuery(self):
        return """MATCH (a:Customer)-[:HAS_PHONE|HAS_SSN|HAS_EMAIL]-(m)-[:HAS_PHONE|HAS_SSN|HAS_EMAIL]-(b:Customer) 
                  RETURN *"""

    def fourthQuery(self, dateStart, dateEnd):
        return f"""MATCH (c:Customer)-[:OPEN]-(claim:Claim)
                   MATCH (claim)-[:DEALS_WITH]-(l:Lawyer)
                   MATCH (claim)-[:CHECK]-(e:Evaluator)
                   WHERE claim.closed_date IS NULL AND 
                   DATETIME(claim.open_date) > DATETIME('{dateStart}') 
                   AND DATETIME(claim.open_date) < DATETIME('{dateEnd}')
                   RETURN *"""
                  
    def save(self):
        Logger.log(f"Salvataggio risultati Neo4J.")
        self.writer.save()
