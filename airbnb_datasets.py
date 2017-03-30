from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
#for assigning neighbourhood
import json
import math

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class airbnb():

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
        #clean dataframe
        df = self.clean_listings(df)
        #set table name
        df.createOrReplaceTempView("listings")
        return df

    #GET DATAFRAME OF REVIEWS
    def get_df_reviews(self):
        data = self.sc.textFile("datasets/reviews_us.csv")
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
        df.createOrReplaceTempView("reviews")

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

    #GET DATAFRAME OF NEIGHBOURHOOD TEST SET
    def get_df_neighborhoods(self):
        data = self.sc.textFile("datasets/neighborhood_test.csv")
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
        df.createOrReplaceTempView("neighborhood_test")

        return df

    #CLEAN DATAFRAME
    def clean_listings(self, df):
        #CLEAN COLUMN "city"
        #trim whitespaces
        df = df.withColumn('city', trim(df.city))

        #CLEAN COLUMN "price"
        #remove dollar sign
        df = df.withColumn("price", regexp_replace("price", "\$", ""))
        df = df.withColumn("price", regexp_replace("price", "\,", ""))

        return df

    #GET DISTINCT AMENITIES FOR EACH NEIGHBORHOOD(6b)
    def get_distinctAmenities(self):
        df_listings = self.get_df_listings()

        #import dataset of listings matched with neighbourhood
        data = self.sc.textFile("datasets/listingsWithNeighbourhood.csv")
        schemaString = data.first().encode('utf-8')
        #remove header
        data = data.filter(lambda line: line != schemaString)
        #set data sample size
        data = data.sample(False, self.sample_size, 7)
        #split into parts
        parts = data.map(lambda l: l.split(","))
        #create string for mapping
        map_string = self.get_mappingString(schemaString).encode('utf-8')
        #make dataframe
        df_neighbourhoodMatch = self.get_dataframe(map_string, schemaString, parts)
        df_neighbourhoodMatch.show()

        #join dataframes
        df_joined = df_listings.join(df_neighbourhoodMatch, df_listings.id == df_neighbourhoodMatch.listings_id)
        #df_joined.createOrReplaceTempView("joined")

        amenities_list = df_joined.select("amenities", "neighbourhood").collect()

        dict_list = []
        for row in amenities_list:
            row_am = row.amenities.replace(" ", ",").replace("''", "").split(",")
            for amenity in row_am:
                dict_list.append({"amenity": str(amenity), "neighbourhood": str(row.neighbourhood)})

        amenities_rdd = self.sc.parallelize(dict_list)
        amenities_df = self.spark.read.json(amenities_rdd)

        df_distinct = am_df.distinct().select("neighbourhood", "amenity")
        results = df_distinct.rdd.groupByKey().mapValues(list).toDF(["neighbourhood", "amenities"])

        results.show()


    #COMPARE RESULTS OF NEIGHBORHOOD ALGORITHM(6a)
    def get_neighborhoodComparison(self):
        df_testing = self.get_df_neighborhoods()

        #get coordinates of listings
        df_listings = self.get_df_listings()

        #ONLY USE LISTINGS COHERENT TO TESTING SET
        coordinates_list = df_listings.join(df_testing, ['id'])

        #get coordinates from listings
        coordinates_list.createOrReplaceTempView("joined")
        coordinates_list = self.spark.sql("SELECT id, latitude, longitude FROM joined").collect()

        with open('datasets/neighbourhoods.geojson') as json_data:
            d = json.load(json_data)

        features = d['features']

        #should contain all coordinates to assign neighbourhood to
        listing_neighbourhood = {}
        for coordinates_listing in coordinates_list:

            #testing only
            current_listing_id = coordinates_listing["id"]

            #print "New listing"
            for neighbourhood in features:
                geometry = neighbourhood['geometry']
                coordinates_neighbourhood = geometry['coordinates'][0][0]
                properties = neighbourhood['properties']
                neighbourhood_group = properties['neighbourhood_group']
                neighbourhood = properties['neighbourhood']

                #make arrays of vertices in direction x(index 0) and y(index 1)
                vertx = []
                verty = []
                for coordinate in coordinates_neighbourhood:
                    vertx.append(float(coordinate[1]))
                    verty.append(float(coordinate[0]))

                #find number of vertices
                nvert = len(vertx)

                #set coordinates of listing
                try:
                    testx = float(coordinates_listing[1])
                    testy = float(coordinates_listing[2])
                except:
                    continue

                #check if in polygon
                inPolygon = False
                c = 0
                j = 1
                a = 0
                for i in range(len(verty)):
                    if (((verty[i] > testy) != (verty[j] > testy)) and (testx < (vertx[j] - vertx[i]) * (testy - verty[i]) / (verty[j] - verty[i]) + vertx[i])):
                        if c == 0:
                            c = 1
                        else:
                            c = 0
                        a += 1
                    j = i + 1
                if c == 1:
                    inPolygon = True

                #assign neighbourhood
                if inPolygon:
                    print neighbourhood
                    listing_neighbourhood[current_listing_id] = neighbourhood
                    break

        #SAVE ALL LISTINGS THAT WAS ASSIGNED NEIGHBOURHOOD
        #make dataframe of results
        #df_results = self.sc.parallelize([([k, v]) for k, v in listing_neighbourhood.items()]).toDF(['listings_id', 'assigned_neighbourhood'])
        #df_results.show()
        #save to file
        #self.save_toFile(df_results, "6a_assign_allListings.csv")
        #return

        #COMPARE RESULTS TO TESTING SET
        #make list of testing set
        neighbourhood_test_list = df_testing.collect()

        #compare results to test set
        count_match = 0
        count_foundNew = 0
        print "\n"
        for test_case in neighbourhood_test_list:
            if str(test_case['id']) in listing_neighbourhood:
                print listing_neighbourhood[str(test_case['id'])]
                print test_case['neighbourhood']
                if listing_neighbourhood[str(test_case['id'])] == str(test_case['neighbourhood']):
                    count_match += 1
                elif len(test_case['neighbourhood']) < 1:
                    count_foundNew += 1


        print "Assigned %d neighbourhoods out of %d" % (len(listing_neighbourhood), len(coordinates_list))
        print "Assigned the right neighbourhood %d out of %d times" % (count_match, len(coordinates_list))
        print "That equals to a %d percent match" % (100.0 * float(count_match)/float(len(coordinates_list)))
        print "Assigned neighbourhood to %d listings not having neighbourhood in testing set" % (count_foundNew)

    #GET THE GUEST THAT SPENT THE MOST MONEY ON ACCOMODATION AND THE AMOUNT(5b)
    def get_mostSpendingGuestAndAmount(self):
        df_listings = self.get_df_listings()
        df_reviews = self.get_df_reviews()

        #join dataframes
        df_joined = df_reviews.join(df_listings, df_reviews.listing_id == df_listings.id)
        df_joined.createOrReplaceTempView("joined")

        #sql
        results = self.spark.sql("SELECT reviewer_id, reviewer_name, SUM(price) AS amountSpent FROM joined GROUP BY reviewer_id, reviewer_name ORDER BY amountSpent DESC LIMIT 1")

        #save to file
        self.save_toFile(results, "5b.csv")

    #GET TOP THREE REVIEWERS FOR EACH CITY(5a)
    def get_topReviewers(self):
        df_reviews = self.get_df_reviews()

        df_listings = self.get_df_listings()
        #filter out city names having length equal to or below 0
        df_listings = df_listings.filter(length("city") > 0)

        #join dataframes
        df_joined = df_reviews.join(df_listings, df_reviews.listing_id == df_listings.id)
        df_joined.createOrReplaceTempView("joined")

        #get bookings
        df_joined = self.spark.sql("SELECT city, reviewer_id, reviewer_name, COUNT(listing_id) AS bookings FROM joined GROUP BY city, reviewer_id, reviewer_name ORDER BY bookings DESC")
        df_joined.createOrReplaceTempView("joined")

        #sql
        results = self.spark.sql("SELECT city, reviewer_id, reviewer_name, bookings FROM (SELECT city, reviewer_id, reviewer_name, bookings, dense_rank() OVER (PARTITION BY city ORDER BY bookings DESC) as rank FROM joined GROUP BY city, reviewer_id, reviewer_name, bookings) tmp WHERE rank <= 3 GROUP BY city, reviewer_id, reviewer_name, bookings")

        #save to file
        self.save_toFile(results, "5a.csv")

    #GET TOP THREE HOSTS WITH HIGHEST INCOME FOR EACH CITY(4c)
    def get_topThree_highestIncome(self):
        df_listings = self.get_df_listings()

        #filter out listings having price of 0 or under
        df_listings = df_listings.filter("price > 0")

        df_calendar = self.get_df_calendar()
        #use only unavailable listings to calculate price
        df_calendar = self.spark.sql("SELECT listing_id, date FROM calendar WHERE available = 'f'")

        #join dataframes
        df_joined = df_calendar.join(df_listings, df_calendar.listing_id == df_listings.id)
        df_joined.createOrReplaceTempView("joined")

        #calculate income
        df_joined = self.spark.sql("SELECT city, host_id, host_name, SUM(price) AS income FROM joined GROUP BY city, host_id, host_name ORDER BY income DESC")

        df_joined.createOrReplaceTempView("joined")
        results = self.spark.sql("SELECT city, host_id, host_name, income FROM (SELECT city, host_id, host_name, income, dense_rank() OVER (PARTITION BY city ORDER BY income DESC) as rank FROM joined GROUP BY city, host_id, host_name, income) tmp WHERE rank <= 3 GROUP BY city, host_id, host_name, income ORDER BY city ASC")

        #save to file
        self.save_toFile(results, "4c.csv")

    #GET GLOBAL AVERAGE NUMBER OF LISTINGS PER HOST(4a)
    def get_numberOf_listingsPerHost(self):
        df = self.get_df_listings()

        #sql
        results = self.spark.sql("SELECT ROUND(COUNT(id)/COUNT(DISTINCT host_id), 2) FROM listings")

        #save to file
        self.save_toFile(results, "4a.csv")

    #GET PERCENTAGE OF HOSTS WITH MORE THAN 1 LISTING(4b)
    def get_percentage_listingsPerHost_moreThanOne(self):
        df = self.get_df_listings()

        #get total hosts
        total_hosts = self.spark.sql("SELECT COUNT(DISTINCT host_id) AS total FROM listings")
        #get hosts having more than one listing
        hosts_moreThanOne = self.spark.sql("SELECT COUNT(DISTINCT host_id) AS part FROM listings WHERE host_listings_count > 1")

        #join dataframes
        df_joined = total_hosts.join(hosts_moreThanOne, total_hosts.total != hosts_moreThanOne.part)
        df_joined.createOrReplaceTempView("joined")

        #sql
        results = self.spark.sql("SELECT ROUND((part/total)*100, 2) AS percentage FROM joined")

        #save to file
        self.save_toFile(results, "4b.csv")



    #GET ESTIMATED AMOUNT SPENT ON ACCOMODATION PER YEAR FOR EACH CITY(3e)
    def get_amountSpent_accomodationPerYear(self):
        df = self.get_df_listings()

        #sql
        results = self.spark.sql("SELECT city, ROUND((((SUM(reviews_per_month)*12)/0.7)*3)*AVG(PRICE), 2) AS accommodationPerYear FROM listings GROUP BY city")

        #save to file
        self.save_toFile(results, "3e.csv")

    #GET ESTIMATED NUMBER OF NIGHTS BOOKED PER YEAR FOR EACH CITY(3d)
    def get_numberOf_nightsBooked(self):
        df = self.get_df_listings()

        #sql
        results = self.spark.sql("SELECT city, ROUND(((SUM(reviews_per_month)*12)/0.7)*3, 2) AS nightsBooked_perYear FROM listings GROUP BY city ")

        #save to file
        self.save_toFile(results, "3d.csv")

    #GET AVERAGE NUMBER OF REVIEWS PER MONTH FOR EACH CITY(3c)
    def get_avg_reviewsPerMonth(self):
        df = self.get_df_listings()

        #sql
        results = self.spark.sql("SELECT city, ROUND(AVG(reviews_per_month), 2) as avg_reviewsPerMonth FROM listings GROUP BY city")

        #save to file
        self.save_toFile(results, "3c.csv")

    #GET AVERAGE BOOKING PRICE PER NIGHT FOR EACH CITY(3a)
    def get_avg_bookingPrice(self):
        df = self.get_df_listings()
        df = df.filter("price > 0")

        #sql
        results = self.spark.sql("SELECT city, ROUND(AVG(price), 2) AS avg_bookingPrice FROM listings GROUP BY city ORDER BY AVG(price) DESC")

        #save to file
        self.save_toFile(results, "3a.csv")

    #GET AVERAGE BOOKING PRICE PER ROOM TYPE PER NIGHT FOR EACH CITY(3b)
    def get_avg_bookingPrice_roomType(self):
        df = self.get_df_listings()
        df = df.filter("price > 0")

        #sql
        results = self.spark.sql("SELECT city, room_type, AVG(price) AS avg_bookingPrice_roomType FROM listings GROUP BY city, room_type")

        #save to file
        self.save_toFile(results, "3b.csv")

    #GET ALL CITY NAMES(2c)
    def get_cityNames(self):
        df = self.get_df_listings()
        df = df.filter(length("city") > 0)

        #sql
        results = self.spark.sql("SELECT city FROM listings GROUP BY city")
        #results.show()

        #save to file
        self.save_toFile(results, "2c_p2.csv")

    #GET NUMBER OF UNIQUE CITY NAMES(2c)
    def get_numberOf_distinct_cityNames(self):
        df = self.get_df_listings()
        df = df.filter(length("city") > 0)

        #sql
        results = self.spark.sql("SELECT COUNT(DISTINCT city) AS numberOf_distinctCities FROM listings")

        #save to file
        self.save_toFile(results, "2c_p1.csv")

    #GET NUMBER OF DISTINCT VALUES IN EACH COLUMN(2b)
    def get_numberOf_distinct_values(self):
        df = self.get_df_listings()

        res_dict = {}
        for c in df.columns:
            #count distinct values in each column
            result = df.agg(countDistinct(c).alias(c)).collect()
            #add to dict
            res_dict[next(iter(result[0].asDict()))] = result[0][0]

        #get dataframe from dict
        results = self.sc.parallelize([([k, v]) for k, v in res_dict.items()]).toDF(['column', 'distinctValues'])

        #save to file
        self.save_toFile(results, "2b.csv")

    #SAVE DATAFRAME TO FILE
    def save_toFile(self, dataframe, filename):
        #get only one output file by running a single partition
        dataframe = dataframe.repartition(1)
        #write to file
        dataframe.write.format("csv").option("header", "true").option("delimiter", ",").save(filename)

if __name__ == "__main__":
    airbnb = airbnb()
    airbnb.get_numberOf_distinct_cityNames()
