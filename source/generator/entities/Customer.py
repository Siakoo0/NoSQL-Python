from .Entity import Entity


class Customer(Entity):
    def __init__(self, fsName, lsName):
        attributes = {
            "first_name": fsName,
            "last_name": lsName,
            "full_name": fsName + " " + lsName
        }

        super().__init__(attributes, "Customer")

        self.setMeta("collection", "customers")
