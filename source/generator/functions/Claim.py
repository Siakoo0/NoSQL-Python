from concurrent.futures import ThreadPoolExecutor

from source.generator.entities.Evaluator import Evaluator
from source.generator.entities.Lawyer import Lawyer
from source.generator.entities.Claim import Claim

from source.tools.Logger import Logger

from datetime import datetime, timedelta
from random import choice, randint

def createClaim(arrayList, relations, indexClaim):
    customer = extract(relations["customers"], indexClaim)
    evaluator = extract(relations["evaluators"], indexClaim)
    lawyer = extract(relations["lawyers"], indexClaim)
    
    relationships = {
        "OPEN" : customer,
        "CHECKS" : evaluator,
        "DEALS_WITH" : lawyer
    }
    
    insuranceTypes = ["Furto in Casa", "Furto Auto", "Incendio doloso Auto", "Incendio doloso Casa", "Incidente Auto"]
    dStart = datetime.now() - timedelta(days=randint(1, 365), minutes=randint(5, 10), seconds=randint(1, 59))

    dEnd = dStart + timedelta(days=5, minutes=randint(5, 10), seconds=randint(1, 59))
    
    claim = Claim(choice(insuranceTypes), dStart, dEnd if choice([True, False]) else "")
    
    for relation, entity in relationships.items():
        claim.link(relation, entity)
    
    arrayList.append(claim)

def extract(array : list, index : int):
    return array[index % len(array)]

def generateClaims(generator):
    claimsData = {
        "lawyers" : [],
        "evaluators" : [],
        "customers" : generator.entities["customers"]
    }
    
    with ThreadPoolExecutor(2) as pool:
        pool.submit(generateLawyers, generator.get("lawyers"), claimsData["lawyers"], generator.fake)    
        pool.submit(generateEvaluators, generator.get("evaluators"), claimsData["evaluators"], generator.fake)    
    
    generator.entities["claims"] = []
    
    with ThreadPoolExecutor(10) as pool:
        for indexClaim in range(generator.get("claims")):
            pool.submit(createClaim, generator.entities["claims"], claimsData, indexClaim)
    
    Logger.log(f"[ Dimensione: {generator.getTotal()} - Percentuale {generator.getPercentage()}% ] Caricamento Claims completato.")

def createLawyer(arrayList : list, fake, IVA):
    fsName = fake.first_name()
    lsName = fake.last_name()
    
    arrayList.append(Lawyer(IVA, fsName, lsName))

def generateLawyers(quantity : int, arrayList : list, fake): 
    IVASet = set()
    
    while len(IVASet) < quantity:
        IVASet.add(fake.company_vat())
    
    IVASet = list(IVASet)
    
    with ThreadPoolExecutor(10) as pool:
        for indexLawyer in range(quantity):
            pool.submit(createLawyer, arrayList, fake, IVASet[indexLawyer])        

def createEvaluators(arrayList : list, fake, code):
    fsName = fake.first_name()
    lsName = fake.last_name()
    
    arrayList.append(Evaluator(code, fsName, lsName))

def generateEvaluators(quantity : int, arrayList : list, fake): 
    codeSet = set()
    
    while len(codeSet) < quantity:
        codeSet.add(fake.random_number(digits=12))
    
    codeSet = list(codeSet)
    
    with ThreadPoolExecutor(10) as pool:
        for indexEvaluator in range(quantity):
            pool.submit(createEvaluators, arrayList, fake, codeSet[indexEvaluator])        

