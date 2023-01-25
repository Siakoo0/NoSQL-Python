from source.generator.Generator import Generator
import os

from source.benchmark.Benchmark import Benchmark

if __name__ == "__main__":
    shutdownBool = True
    
    for nCustomers in [40000, 50000]:
        generator = Generator(nCustomers)
        datasetDimension = generator.getTotal()
        b = Benchmark(datasetDimension)

        for percentage in range(25, 101, +25):
            generator.generate(percentage)
            b.start(percentage)
            
    if shutdownBool:
        os.system("shutdown /s /t 1")