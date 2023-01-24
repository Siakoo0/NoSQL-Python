class Entity:
    def __init__(self, attributes, type):
        self.attributes = attributes
        self.__meta = {"type": type}
        self.__rel = dict()

    def setAttributes(self, key, value):
        self.attributes[key] = value

    def getAttributes(self):
        return self.attributes

    def getRelationships(self):
        return self.__rel

    def getRelation(self, key):
        return self.__rel[key]

    def setMeta(self, key, value):
        self.__meta[key] = value

    def hasMeta(self, key):
        return key in self.__meta.keys()

    def link(self, relationship, entity):
        self.__rel[relationship] = entity

    def getMeta(self, key):
        if key in self.__meta.keys():
            return self.__meta[key]
        else:
            return None
