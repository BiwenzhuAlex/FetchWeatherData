import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

reader = DataFileReader(open("test.avro", "rb"), DatumReader())
count = 0
for weather_by_hrs_by_region  in reader:
   print (weather_by_hrs_by_region)
   count += 1
print ("********")
print(count)
reader.close()

