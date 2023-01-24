from .Entity import Entity


class Address(Entity):
    def __init__(self, country, city, street, zipCode):
        attributes = {
            "country": country,
            "city": city,
            "street": street,
            "zip_code": zipCode
        }

        super().__init__(attributes, "Address")
