from .Entity import Entity


class Evaluator(Entity):
    def __init__(self, code, fsName, lsName):
        attributes = {
            "code": code,
            "first_name": fsName,
            "last_name": lsName
        }

        super().__init__(attributes, "Evaluator")

