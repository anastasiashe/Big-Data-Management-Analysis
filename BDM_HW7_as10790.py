
# coding: utf-8

from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import unix_timestamp
from pyspark import SparkContext
from pyspark.sql import HiveContext


CB_FN = '/tmp/citibike.csv'
Taxi_FN = '/tmp/yellow.csv'

bike = sc.textFile(CB_FN, use_unicode=False).cache() 
taxi = sc.textFile(Taxi_FN, use_unicode=False).cache()

def extractRides(partId, parts): # parts is a list of records 
    if partId==0:
        parts.next() #skip the first line 
    import csv
    reader = csv.reader(parts)
    for row in reader:
        date = row[3].split(' ')[0]
        time = row[3].split(' ')[1].split('+')[0]
        station, lat, lon = row[6], row[7], row[8]
        if '2015-02-01' in date and station == 'Greenwich Ave & 8 Ave':
            yield Row(time)

def extractTrips(partId, parts): # parts is a list of records
    if partId==0:
        parts.next() # skip the first line
    import csv
    reader = csv.reader(parts)
    for row in reader:
        import pyproj
        proj = pyproj.Proj(init = 'EPSG:2263', preserve_units = True)
        if row[5] != 'NULL' and row[5] != 0:
            time, lonlat = row[0].split(' ')[1].split('.')[0], proj(row[5], row[4])
            G8 = (983519.0693404069, 208520.40726307002)
            dist = ((lonlat[0] - G8[0]) ** 2 + (lonlat[1] - G8[1]) ** 2) ** 0.5
            if dist < 1320.0: # 0.25 miles is 1320 feet
                yield Row(time)

def main(sc):
    spark = HiveContext(sc)

    brides = bike.mapPartitionsWithIndex(extractRides) # similar to map partitions, allows to skip the header row
    ttrips = taxi.mapPartitionsWithIndex(extractTrips) # similar to map partitions, allows to skip the header row

    brides_df = brides.toDF(['rides'])
    ttrips_df = ttrips.toDF(['trips'])

    timeFmt = "HH:mm:ss"
    interval = unix_timestamp(brides_df.rides, format = timeFmt) - unix_timestamp(ttrips_df.trips, format = timeFmt)

    ridesTrips = brides_df.join(ttrips_df).filter((interval >= 0) & (interval <= 600)).select('rides')

    print ridesTrips.dropDuplicates().count()

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)


