from faker import Faker

from .functions.Customer import generateCustomers 
from .functions.Claim import generateClaims

from source.generator.writers.DatabaseWriter import write

class Generator:
    def __init__(self, customers) -> None:
        self.params = {}
        self.percentage = 1
        self.fake = Faker("it_IT")
        self.entities = {}
        
        self.initialize(customers)
        
    def generate(self, percentage):
        self.percentage = percentage / 100    
        
        generateCustomers(self)
        generateClaims(self)
        
        write(self.entities, self.getPercentage(), self.getTotal())
        
    def getTotal(self) -> int:
        total = 0
        for quantity in self.params.values():
            total += quantity
        return total
        
    def getPercentage(self) -> str:
        perc = int(self.percentage * 100)
        return str(perc)
        
    def get(self, key):
        return int(self.params[key] * self.percentage)        
        
    def initialize(self, customers):
        self.params["customers"] = customers
        
        self.params["addresses"] =  customers * 0.9
        self.params["phones"] =  customers * 0.7
        self.params["emails"] =  customers * 0.8
        self.params["SSNs"] =  customers * 0.85
        
        self.params["claims"] = customers * 1.2
        self.params['lawyers'] = self.params["claims"] * 0.8
        self.params['evaluators'] = self.params["claims"] * 0.9
        
        for key, value in self.params.items():
            self.params[key] = int(value)
            
    