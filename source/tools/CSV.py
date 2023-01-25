import csv
from pathlib import Path

class CSV:
    def __init__(self, name):
        self.reader = None
        self.writer = None
        self.name = name

    def write(self, path, rows):
        Path(path).mkdir(parents=True, exist_ok=True, mode=0o777)
        
        rowsToWrite = {}
        
        for row in rows:
            headerSet = tuple(row.keys())
            if not headerSet in rowsToWrite:
                rowsToWrite[headerSet] = []
            rowsToWrite[headerSet].append(row) 
        
        for indexFile, headers in enumerate(rowsToWrite.keys()):
            filename = path + self.name if indexFile == 0 else path + self.name + "_" + str(indexFile)
            with open(filename + ".csv", 'w', newline='') as file:
                writer = csv.DictWriter(file, list(headers), delimiter=";")
                writer.writeheader()
                for row in rowsToWrite[headers]:
                    writer.writerow(row)
        