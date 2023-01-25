from source.generator.entities.Customer import Customer
from source.generator.entities.Address import Address
from source.generator.entities.DataCustomer import DataCustomer

from concurrent.futures import ThreadPoolExecutor

from source.tools.Logger import Logger

from random import choice,randint

def createAddress(arrayList : list, fake) -> None:
    country = fake.current_country()
    city = fake.state()
    zipCode = fake.postcode()
    streetAddress = fake.street_address()
    
    arrayList.append(Address(country, city, streetAddress, zipCode))

def generateAddresses(quantity : int, arrayList : list, fake):
    with ThreadPoolExecutor(10) as pool:
        for _ in range(quantity):
            pool.submit(createAddress, arrayList, fake)

def createEmail(arrayList: list, fake) -> DataCustomer:
    domains = ["outlook.it", "outlook.com", "libero.it", "hotmail.com", "gmail.com", "hotmail.it", "yahoo.com"]
    email = f"{fake.first_name().lower()}.{fake.last_name().lower()}@{choice(domains)}"
    
    while email in arrayList:
        email = f"{fake.first_name().lower()}.{fake.last_name().lower()}@{choice(domains)}"
        
    arrayList.append(DataCustomer("Email", email))

def generateEmail(quantity : int, arrayList : list, fake) -> None:
    with ThreadPoolExecutor(10) as pool:
        for _ in range(quantity):
            pool.submit(createEmail, arrayList, fake)

def createPhone(arrayList : list):
    numberPhone = str(randint(393739025644, 393899999999))
    
    while numberPhone in arrayList:
        numberPhone = str(randint(393739025644, 393899999999))
    
    arrayList.append(DataCustomer("Phone", numberPhone))

def generatePhone(quantity : int, arrayList : list) -> None:
    with ThreadPoolExecutor(10) as pool:
        for _ in range(quantity):
            pool.submit(createPhone, arrayList)

def createSSN(arrayList: list, fake):
    SSNstring = fake.ssn()
    
    while SSNstring in arrayList:
        SSNstring = fake.ssn()
    
    arrayList.append(DataCustomer("SSN", SSNstring))

def generateSSN(quantity : int, arrayList : list, fake) -> None:
    with ThreadPoolExecutor(10) as pool:
        for _ in range(quantity):
            pool.submit(createSSN, arrayList, fake)

def extract(array : list, index : int):
    return array[index % len(array)]

def generateCustomers(generator) -> None:
    customerData = {
        "addresses" : [],
        "emails" : [],
        "phones" : [],
        "SSNs" : []
    }
    
    with ThreadPoolExecutor(4) as pool:
        pool.submit(generateAddresses, generator.get("addresses"), customerData["addresses"], generator.fake)
        pool.submit(generatePhone, generator.get("phones"), customerData["phones"])
        pool.submit(generateEmail, generator.get("emails"), customerData["emails"], generator.fake)
        pool.submit(generateSSN, generator.get("SSNs"), customerData["SSNs"], generator.fake)
    
    generator.entities["customers"] = []
    
    for customerIndex in range(generator.get("customers")):        
        with ThreadPoolExecutor(10) as pool:
            pool.submit(createCustomer, generator.entities["customers"], generator.fake, customerData, customerIndex)
    
    Logger.log(f"[ Dimensione: {generator.getTotal()} - Percentuale {generator.getPercentage()}% ] Caricamento Customers completato.")
    
def createCustomer(arrayList: list, fake, customerData : dict, customerIndex) -> None:
    address = extract(customerData["addresses"], customerIndex)
    phone = extract(customerData["phones"], customerIndex)
    email = extract(customerData["emails"], customerIndex)
    ssn = extract(customerData["SSNs"], customerIndex)
    
    relations = {
        "HAS_ADDRESS" : address,
        "HAS_EMAIL" : email,
        "HAS_PHONE" : phone,
        "HAS_SSN" : ssn,
    }
    
    fsName = fake.first_name()
    lsName = fake.last_name()
    
    customer = Customer(fsName, lsName)
    
    for relation, entity in relations.items():
        customer.link(relation, entity)
    
    arrayList.append(customer)
    
    