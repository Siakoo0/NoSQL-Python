from source.database.MongoDB import MongoDB
from source.tools.ResultWriter import ResultWriter
from datetime import datetime, timedelta
from source.tools.Logger import Logger
import time

cronometer = lambda : int(round(time.time() * 100))

class MongoBench:
    def __init__(self, dimension):
        self.db = MongoDB.db
        self.writer = ResultWriter(dimension, "MongoDB")

    def firstQuery(self):
        start = cronometer()

        res = self.db.claims.aggregate(
            [
                {"$match": {"closed_date": {"$exists": True}}},
                {"$project": {"customer": 0, "lawyer": 0, "evaluator": 0}}
            ],
            allowDiskUse=True
        )
        
        res = list(res)
            
        return cronometer() - start

    def secondQuery(self):
        start = cronometer()
        res = self.db.claims.aggregate(
            [
                {"$project": {"lawyer": 0, "evaluator": 0}},
                {
                    "$match": {
                        "closed_date": {"$exists": False}
                    }
                },
                {
                    "$lookup": {
                        "from": "customers",
                        "localField": "customer",
                        "foreignField": "_id",
                        "as": "customer"
                    }
                },
            ],
            allowDiskUse=True
        )
        
        res = list(res)
        
        return cronometer() - start

    def thirdQuery(self):
        dateStart = datetime.now() - timedelta(days=15)
        dateEnd = datetime.now() + timedelta(days=15)
        
        start = cronometer()

        res = self.db.claims.aggregate(
            [
                {
                    "$match": {
                        "closed_date": {"$exists": False},
                        "open_date": {
                            "$gt": dateStart, "$lt": dateEnd
                        }
                    }
                },
                {
                    "$lookup": {
                        "from": "customers",
                        "localField": "customer",
                        "foreignField": "_id",
                        "as": "customer"
                    }
                },
            ],
            allowDiskUse=True
        )
        res = list(res)

        return cronometer() - start

    def fourthQuery(self):
        start = cronometer()

        res = self.db.customers.aggregate(
            [
                {
                    "$facet": {
                        "email": [
                            {"$group": {"_id": "$email", "customers": {"$push": "$$ROOT"}, "count": {"$sum": 1}}},
                            {"$match": {"count": {"$gte": 2}}},
                            {"$project": {"_id": 1, "customers": 1}},
                        ],
                        "phone": [
                            {"$group": {"_id": "$phone", "customers": {"$push": "$$ROOT"}, "count": {"$sum": 1}}},
                            {"$match": {"count": {"$gte": 2}}},
                            {"$project": {"_id": 1, "customers": 1}},
                        ],
                        "SSN": [
                            {"$group": {"_id": "$SSN", "customers": {"$push": "$$ROOT"}, "count": {"$sum": 1}}},
                            {"$match": {"count": {"$gte": 2}}},
                            {"$project": {"_id": 1, "customers": 1}},
                        ]
                    }
                },
                {
                    "$project": {"unionSet": {"$setUnion": ["$email", "$phone", "$SSN"]}}
                },
                {
                    "$unwind": "$unionSet"
                },
                {
                    "$replaceRoot": {"newRoot": "$unionSet"}
                },
                {
                    "$group": {"_id": "$customers", "repeatedField": {"$push": "$$ROOT"}}
                },
                {
                 "$project" : {"_id": 0, "customers": "$_id", "repeatedField" : "$repeatedField._id"}
                },
                {
                    "$unwind" : "$repeatedField"
                }
            ],
            allowDiskUse=True
        )
        
        res = list(res)

        return cronometer() - start

    def benchmark(self, percentage):
        results = []

        MongoDB.clearCache()

        for _ in range(31):
            results.append(
                [
                    self.firstQuery(),
                    self.secondQuery(),
                    self.thirdQuery(),
                    self.fourthQuery()
                ]
            )

        self.writer.writeResults(str(percentage), results)
        Logger.log(f"Scrittura risultati MongoDB percentuale {percentage}%.")

    def save(self):
        Logger.log(f"Salvataggio risultati MongoDB.")
        self.writer.save()