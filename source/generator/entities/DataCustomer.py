from .Entity import Entity


class DataCustomer(Entity):
    def __init__(self, tipo, value):
        attributes = {
            "value": value
        }

        super().__init__(attributes, tipo)
