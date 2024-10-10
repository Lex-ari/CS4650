
import re
import json

from mrjob.job import MRJob

class AlphabetPartOne(MRJob):
    
    def mapper(self, _, line):
        data = line.strip()
        values = data.split(",")
        row = values[0]
        col = values[1]
        val = int(values[2])
        yield row, val
        yield col, val
    
    def reducer(self, key, values):
        if key >= "A" and key <= "J":
            yield key, max(values)
        else:
            yield key, min(values)
        
    
if __name__ == '__main__':
    AlphabetPartOne.run()
