from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

import math
import string

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class alternative_listings():
    #x: price is not higher by more than x%
    #y: is located within y km
    #n: top n outputs

    def __init__(self):
        #SET SPARKCONTEXT
        conf = SparkConf().setAppName("AirBnB datasets")
        self.sc = SparkContext(conf=conf)

        #INITIALIZE SPARK SESSION
        self.spark = SparkSession \
            .builder \
            .getOrCreate()

        #SET LOGGING LEVEL TO ERROR ONLY
        self.spark.sparkContext.setLogLevel('ERROR')

        #SET SAMPLE SIZE TO EXTRACT
        self.sample_size = 1

    #GET STRING FOR MAPPING
    def get_mappingString(self, schemaString):
        col_size = len(schemaString.split())
        map_string = ""
        for i in range(col_size):
            map_string += "p[%d]" % i
            if i + 1 < col_size:
                map_string += ", "
        return map_string

    #GET DATAFRAME FROM MAPPING STRING
    def get_dataframe(self, map_string, schemaString, parts):
        details = parts.map(lambda p: (eval(map_string)))
        print details
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)
        df = self.spark.createDataFrame(details, schema)
        return df

    #GET DATAFRAME OF LISTINGS
    def get_df_listings(self):
        data = self.sc.textFile("datasets/listings_us.csv")
        schemaString = data.first()
        #remove header
        data = data.filter(lambda line: line != schemaString)
        #set data sample size
        data = data.sample(False, self.sample_size, 7)
        #split into parts
        parts = data.map(lambda l: l.split("\t"))
        #create string for mapping
        map_string = self.get_mappingString(schemaString)
        #create dataframe
        df = self.get_dataframe(map_string, schemaString, parts)
        #set table name
        df.createOrReplaceTempView("listings")
        return df

    #GET DATAFRAME OF CALENDAR
    def get_df_calendar(self):
        data = self.sc.textFile("datasets/calendar_us.csv")
        schemaString = data.first()
        #remove header
        data = data.filter(lambda line: line != schemaString)
        #set data sample size
        data = data.sample(False, self.sample_size, 7)
        #split into parts
        parts = data.map(lambda l: l.split("\t"))
        #create string for mapping
        map_string = self.get_mappingString(schemaString)
        #make dataframe
        df = self.get_dataframe(map_string, schemaString, parts)
        #set table name
        df.createOrReplaceTempView("calendar")
        return df

    def run(self, args):
        #set parameters
        self.listing_id = args[1]
        self.date = "'" + args[2] + "'"
        self.price_limit = args[3]
        self.max_distance = args[4]
        self.outputs = args[5]

        df_listings = self.get_df_listings()
        #clean price
        df_listings = df_listings.withColumn("price", regexp_replace("price", "\$", ""))
        df_listings = df_listings.withColumn("price", regexp_replace("price", "\,", ""))
        #extract relevant columns
        df_listings = df_listings.select("id", "name", "room_type", "price", "latitude", "longitude", "amenities", "beds", "square_feet")

        #get listing data
        self.listing = df_listings.filter("id = %s" % self.listing_id).select("room_type", "price", "latitude", "longitude", "amenities").collect()
        if len(self.listing) > 0:
            self.listing = self.listing[0]
        else:
            raise ValueError("Data for listing_id %s is unavailable" % self.listing_id)

        #get room_type
        self.room_type = "'" + self.listing["room_type"] + "'"
        #extract listings with given room_type
        df_listings = df_listings.filter("room_type = %s" % self.room_type)

        #get price
        self.price = self.listing["price"]
        #calculate max price
        self.max_price = float(self.price) * (1 + float(self.price_limit) * 0.01)
        #extract listings having price below or equal to max_price
        df_listings = df_listings.filter("price <= %d" % self.max_price)

        #get latitude
        self.latitude = self.listing["latitude"]
        #get longitude
        self.longitude = self.listing["longitude"]
        #GET DISTANCE BETWEEN COORDINATES
        latitude = self.latitude
        longitude = self.longitude
        def get_distance(lat1, lon1):
            #from assignment
            # convert decimal degrees to radians
            lat2 = latitude
            lon2 = longitude
            lat1 = float(lat1) * float(math.pi) / 180
            lon1 = float(lon1) * float(math.pi) / 180
            lon2 = float(lon2) * float(math.pi) / 180
            lat2 = float(lat2) * float(math.pi) / 180
            # haversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            cs = 2 * math.asin(math.sqrt(a))
            rs = 6371 # Radius of earth in kilometers. Use 3956 for miles
            distance = cs * rs
            return distance

        #set custom function
        udfDistance = udf(get_distance, StringType())
        #calculate distance to new column
        df_listings = df_listings.select("*", (udfDistance(col('latitude'), col('longitude'))).alias("distance"))
        #extract listings within given vicinity
        df_listings = df_listings.filter("distance <= %d" % int(self.max_distance))

        #make lower case and remove punctuation
        def clean_amenities(amenities):
            amenities = amenities.encode('utf-8').lower().split(",")
            amenities = [amenity.translate(None, string.punctuation) for amenity in amenities]
            return amenities

        #get amenities
        self.amenities = clean_amenities(self.listing["amenities"])

        #count common amenities
        listing_amenities = self.amenities
        def get_commonAmenities(amenities):
            amenities = clean_amenities(amenities)
            count = 0
            for amenity in amenities:
                if amenity in listing_amenities:
                    count += 1
            return count

        #set custom function
        udfAmenities = udf(get_commonAmenities, StringType())
        #calculate common amenities to new column
        df_listings = df_listings.select("*", (udfAmenities(col('amenities'))).alias("commonAmenities"))
        df_listings = df_listings.withColumn("commonAmenities", df_listings.commonAmenities.cast("int"))
        #order by number of common amenities
        df_listings = df_listings.orderBy("commonAmenities", ascending=False)

        #make desired format
        df_results = df_listings.select(col("id").alias("listing_id"), col("name").alias("listing_name"),
                                            col("commonAmenities").alias("number_of_common_amenities"),
                                            col("distance"), col("price"))

        #extract desired number of listings
        df_results = df_results.limit(int(self.outputs))

        #save alternative listings to file
        '''
        #get only one output file by running a single partition
        df_results = df_results.repartition(1)
        #write to file
        df_results.write.format("tsv").option("header", "true").option("delimiter", "\t").save("alternatives.tsv")
        '''

        #save original listing to file for use in vizualisation
        '''
        df_listings = self.get_df_listings()
        df_results = df_listings.filter("id = %s" % self.listing_id).select("id", "name", "price", "beds", "square_feet", "latitude", "longitude")
        df_results = df_results.repartition(1)
        df_results.write.format("tsv").option("header", "true").option("delimiter", "\t").save("original_listing_carto.tsv")
        '''

        #save alternative listings to file for use in vizualisation
        df_results = df_listings.select(col("id"), col("name"), col("price"), col("beds"), col("square_feet"), col("latitude"), col("longitude"))
        df_results = df_results.repartition(1)
        df_results.write.format("csv").option("header", "true").option("delimiter", "\t").save("alternative_listings_carto.tsv")


if __name__ == "__main__":
    #print str(sys.argv)
    al = alternative_listings()
    al.run(sys.argv)
    #al = alternative_listings(15359479, 2016-12-15, 10, 2, 20)

    #al.run()
