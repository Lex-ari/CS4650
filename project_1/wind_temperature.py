
import re
import json

from mrjob.job import MRJob

QUALITY_RE = re.compile(r"[01459]")

class WindTemperature(MRJob):
    
    def mapper(self, _, line):
        val = line.strip()
        (wind_heading, wind_quality, temp) = (val[60:63], val[63], val[87:92])
        if (temp != "+9999" 
            and re.match(QUALITY_RE, wind_quality)
            and wind_heading != "999"):
            yield wind_heading, {"temp":int(temp), "count":1}
    
    def reducer(self, key, values):
        example = ''
        count = 0
        min_temp = None
        max_temp = None
        for reading in values:
            reading_temp = reading["temp"]
            if min_temp is None:
                min_temp = reading_temp
            if max_temp is None:
                max_temp = reading_temp
            min_temp = min(min_temp, reading_temp)
            max_temp = max(max_temp, reading_temp)
            count += reading["count"]
        yield key, {"low":min_temp, "high":max_temp, "count":count}
            
    
if __name__ == '__main__':
    WindTemperature.run()
