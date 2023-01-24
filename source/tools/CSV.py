import csv
from pathlib import Path

class CSV:
    def __init__(self, name):
        self.reader = None
        self.writer = None
        self.name = name + ".csv"

    def write(self, path, rows):
        Path(path).mkdir(parents=True, exist_ok=True, mode=0o777)
        
        headers = {}
        
        for row in rows:
            headers = headers | row.keys()
        
        with open(path + self.name, 'w', newline='') as file:
            writer = csv.DictWriter(file, headers, delimiter=";")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def read(self):
        with open(self.name, 'r') as file:
            reader = csv.DictReader(file)
            return reader