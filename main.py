from source.generator.Generator import Generator

from source.benchmark.Benchmark import Benchmark

if __name__ == "__main__":
    generator = Generator(10)
    b = Benchmark(generator.getTotal())

    for percentage in range(25, 101, +25):
        generator.generate(percentage)
        b.start(percentage)