from .Entity import Entity


class Claim(Entity):
    def __init__(self, type, openDate, closedDate):
        attributes = {
            "type": type,
            "open_date": openDate,
        }

        if not isinstance(closedDate, str):
            attributes["closed_date"] = closedDate

        super().__init__(attributes, "Claim")

        self.setMeta("collection", "claims")
        self.setMeta("reverse_relations", 1)
