from .Entity import Entity


class Lawyer(Entity):
    def __init__(self, IVA, fsName, lsName):
        attributes = {
            "IVA": IVA,
            "first_name": fsName,
            "last_name": lsName
        }

        super().__init__(attributes, "Lawyer")
