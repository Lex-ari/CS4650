
import re
import json

from mrjob.job import MRJob

class AlphabetPartThree(MRJob):
    
    def mapper(self, _, line):
        data = line.strip()
        values = data.split(",")
        row = values[0]
        col = values[1]
        val = int(values[2])
        yield row, {"value":val, "loc":col}
        yield col, {"value":val, "loc":row}
    
    def reducer(self, key, values):
        data_value = None
        min_data = 9999
        max_data = -9999
        loc_min = []
        loc_max = []
        for value in values:
            data_value = value["value"]
            loc_value = value["loc"]
            
            if data_value == min_data:
                loc_min.append(loc_value)
            elif data_value == max_data:
                loc_max.append(loc_value)
            elif data_value < min_data:
                min_data = data_value
                loc_min = [loc_value]
            elif data_value > max_data:
                max_data = data_value
                loc_max = [loc_value]
            
        if key >= "A" and key <= "J":
            yield key, {"value":max_data, "examples":loc_max}
        else:
            yield key, {"value":min_data, "examples":loc_min}
        
    
if __name__ == '__main__':
    AlphabetPartThree.run()
