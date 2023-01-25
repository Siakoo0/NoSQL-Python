from source.database.Database import Database

from source.benchmark.MongoBench import MongoBench
from source.benchmark.Neo4JBench import Neo4JBench

class Benchmark:
    def __init__(self, dimension) -> None:
        Database.clearCache()
        self.dimension = dimension
        self.databases = [
            MongoBench(dimension),
            Neo4JBench(dimension)
        ]
        
    def start(self, percentage):
        for database in self.databases:
            database.benchmark(percentage)
            database.save()
        