from source.generator.Generator import Generator
import os

from source.benchmark.Benchmark import Benchmark

if __name__ == "__main__":
    for nCustomers in [10, 50000]:
        generator = Generator(nCustomers)
        datasetDimension = generator.getTotal()
        b = Benchmark(datasetDimension)

        for percentage in range(25, 101, +25):
            generator.generate(percentage)
            b.start(percentage)