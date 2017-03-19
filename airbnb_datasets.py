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

#TODO: Remove all createOrReplaceTempView, this should be set in get_df_[dataset] functions
#TODO: Set column names in SQL quieries for nice output, ex. "SELECT AVG(price) AS avg_Price"
#TODO: Remove headers in get_df_[dataset] functions
#TODO: Remove(?) cleaning of "city" in listings in functions (new datasets should be clean), maybe in clean_listings as well

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
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)
        df = self.spark.createDataFrame(details, schema)
        return df

    #GET DATAFRAME OF LISTINGS
    def get_df_listings(self):
        #df = self.spark.read.csv("listings_us.csv", header=True, sep="\t")
        #df = df.sample(False, self.sample_size, 7)
        #df.select("city").show()
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

    def get_df_reviews(self):
        #df = self.spark.read.csv("reviews_us.csv", header=True, sep="\t")
        #df = df.sample(False, self.sample_size, 7)
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

    def get_df_calendar(self):
        #df = self.spark.read.csv("reviews_us.csv", header=True, sep="\t")
        #df = df.sample(False, self.sample_size, 7)
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
        #make lowercase
        #df = df.withColumn('city', lower(df.city))

        #CLEAN COLUMN "price"
        #remove dollar sign
        df = df.withColumn("price", regexp_replace("price", "\$", ""))
        df = df.withColumn("price", regexp_replace("price", "\,", ""))

        return df

    #GET FOREIGN KEYS(2a)
    def get_foreignKeys(self):
        df_listings = self.get_df_listings()
        df_listings.printSchema()
        df_reviews = self.get_df_reviews()
        df_reviews.printSchema()
        df_calendar = self.get_df_calendar()
        df_calendar.printSchema()
        df_neighbourhoods = self.get_df_neighborhoods()
        df_neighbourhoods.printSchema()
        df_neighbourhoods.show()
        return

    #FIND DISTINCT AMENITIES FOR EACH NEIGHBORHOOD(6b)
    def get_distinctAmenities(self):
        df_testing = self.get_df_listings()
        df_listings.select("amenities").show()


    #COMPARE RESULTS OF NEIGHBORHOOD ALGORITHM(6a)
    def get_neighborhoodComparison(self):
        df_testing = self.get_df_neighborhoods()
        df_testing.show()



        #get coordinates of listings
        df_listings = self.get_df_listings()
        coordinates_list = self.spark.sql("SELECT id, latitude, longitude FROM listings").collect()

        #remove testings without neighbourhood
        #df_testing = df_testing.filter(length("neighbourhood") > 2)

        #only use listings mentioned in testing test
        #coordinates_list = df_testing.join(df_listings, ['id'])

        #extract limited sample from joined dataframe
        #coordinates_list.createOrReplaceTempView("joined")
        #coordinates_list = self.spark.sql("SELECT id, latitude, longitude FROM joined LIMIT 100").collect()
        #coordinates_list = self.spark.sql("SELECT id, latitude, longitude FROM listings ORDER BY id ASC LIMIT 2").collect()

        #print coordinates_list[0]['id']
        #coordinates_list.show()
        #find properties of listing number index
        #index = 0
        #listing_id = coordinates_list[index][0]
        #listing_lat = coordinates_list[index][1]
        #listing_lon = coordinates_list[index][2]

        with open('datasets/neighbourhoods.geojson') as json_data:
            d = json.load(json_data)

        #just setting up pseudo.
        #coordinates_list are list of coordinates to be assigned to neighbourhoods
        #need dict containing coordinates with key as listing_id
        #result is dict or list with listing_id, neighbourhood_group(city?) and neighbourhood
        #testing sample given by lecturer contains city as well
        #I believe neighbourhood_group equals city

        #use small sample
        features = d['features']
        #features = features[:10]
        #print features[0]['properties']

        #print coordinates_list[0][0]

        #for listing in coordinates_list:
        #    if listing["id"] == 3335:
        #        print "found it"

        #should contain all coordinates to assign neighbourhood to
        listing_neighbourhood = {}
        for coordinates_listing in coordinates_list:

            #testing only
            current_listing_id = coordinates_listing["id"]

            print "New listing"
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

                #print "verty", verty[nvert - 1]

                #set coordinates of listing
                testx = float(coordinates_listing[1])
                testy = float(coordinates_listing[2])

                #print "vertx", vertx[0]
                #print "verty", verty[0]
                #print "testx", testx
                #print "testy", testy

                #check if in polygon
                inPolygon = False
                c = 0
                #j = nvert - 1
                #j = 1
                j = 1
                a = 0
                for i in range(len(verty)):
                    #print "running for loop"
                    #print j
                    #j = i + 1
                    #if len(verty) >= j:
                    if (((verty[i] > testy) != (verty[j] > testy)) and (testx < (vertx[j] - vertx[i]) * (testy - verty[i]) / (verty[j] - verty[i]) + vertx[i])):
                        if c == 0:
                            c = 1
                        else:
                            c = 0
                        a += 1
                    j = i + 1
                #print a
                if c == 1:
                    inPolygon = True

                #assign neighbourhood
                if inPolygon:
                    print neighbourhood
                    listing_neighbourhood[current_listing_id] = neighbourhood
                    break

            #if not inPolygon:
            #    listing_neighbourhood[current_listing_id] = None
                #else:
                #    listing_neighbourhood[current_listing_id] = None
                #print listing_neighbourhood

        #make dataframe of results
        df_results = self.sc.parallelize([([k, v]) for k, v in listing_neighbourhood.items()]).toDF(['listings_id', 'assigned_neighbourhood'])
        df_results.show()
        #save to file
        self.save_toFile(df_results, "6a_assign_allListings.csv")
        return

        #join results with testing set
        df_joined = df_testing.join(df_results, df_results.listings_id == df_testing.id)
        df_joined.show()
        return

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

        print listing_neighbourhood

        for id, neighbourhood in listing_neighbourhood.items():
            print id
            print neighbourhood

        df_result = self.sc.parallelize([([k, v]) for k, v in listing_neighbourhood.items()]).toDF(['id', 'neighbourhood'])
        #df_result = self.sc.parallelize([(list(k), ) + v[0] for k, v in listing_neighbourhood.items()]).toDF(['key', 'val_1'])
        df_result.show()

        print "Assigned %d neighbourhoods" % len(listing_neighbourhood)
        print "Assigned the right neighbourhood %d out of %d times" % (count_match, len(coordinates_list))
        print "Assigned neighbourhood to %d listings not having neighbourhood in testing set" % (count_foundNew)
        #print neighbourhood_test_list[0]
        #print neighbourhood_test_list[0]['neighbourhood']
        return

        #df_geo = self.spark.read.json("neighbourhoods.geojson")
        #df_geo.printSchema()


    #GET THE GUEST THAT SPENT THE MOST MONEY ON ACCOMODATION AND THE AMOUNT(5b)
    def get_mostSpendingGuestAndAmount(self):
        df_listings = self.get_df_listings()
        df_reviews = self.get_df_reviews()

        df_joined = df_reviews.join(df_listings, df_reviews.listing_id == df_listings.id)
        df_joined.createOrReplaceTempView("joined")

        results = self.spark.sql("SELECT reviewer_id, reviewer_name, SUM(price) AS amountSpent FROM joined GROUP BY reviewer_id, reviewer_name ORDER BY amountSpent DESC LIMIT 1")

        #save to file
        self.save_toFile(results, "5b.csv")

    #GET TOP THREE REVIEWERS FOR EACH CITY(5a)
    def get_topReviewers(self):
        df_reviews = self.get_df_reviews()
        #df_calendar = self.get_df_calendar()
        #df_calendar.show()

        #calculate bookings, was DISTINCT listing_id
        #self.spark.sql("SELECT reviewer_id, reviewer_name, COUNT(listing_id) AS bookings FROM reviews GROUP BY reviewer_id, reviewer_name ORDER BY bookings DESC").show()
        #return
        '''
        output sample = 1:
        +-----------+-------------+--------+
        |reviewer_id|reviewer_name|bookings|
        +-----------+-------------+--------+
        |     197711|        J. B.|      68|
        |     206203|       Amanda|      38|
        |    2539165|         Andy|      31|
        |    4236708|     Adrienne|      28|
        |    6257773|        Qiana|      25|
        |   23629872|      Michael|      25|
        |   27102779|      Michael|      24|
        |     472866|        Kevin|      23|
        |   26152563|         Jeff|      22|
        |   28774703|      Richard|      21|
        |    7229438|     Lorraine|      21|
        |   18146200|         Mike|      21|
        |   25701080|        Roger|      21|
        |   15121499|      Kathryn|      21|
        |    5943820|     Tendekai|      20|
        |      14438|          Ron|      20|
        |     255431|        Laura|      20|
        |   19997910|       Innate|      20|
        |   39274139|          Van|      19|
        |    2734499|        Jason|      19|
        +-----------+-------------+--------+
        '''

        #df_review = df_review.join(df_bookings, df_review.reviewer_id == df_bookings.reviewer_id)
        #df_reviews.show()

        #df_review = self.spark.sql("SELECT reviewer_name, listing_id, COUNT(id) AS review_count FROM reviews GROUP BY reviewer_name, listing_id ORDER BY COUNT(id) DESC")

        df_listings = self.get_df_listings()
        df_listings = df_listings.filter(length("city") > 0)

        #join dataframes
        df_joined = df_reviews.join(df_listings, df_reviews.listing_id == df_listings.id)

        df_joined.createOrReplaceTempView("joined")
        df_joined = self.spark.sql("SELECT city, reviewer_id, reviewer_name, COUNT(listing_id) AS bookings FROM joined GROUP BY city, reviewer_id, reviewer_name ORDER BY bookings DESC")
        #df_joined.show()
        df_joined.createOrReplaceTempView("joined")
        #return

        #sql
        results = self.spark.sql("SELECT city, reviewer_id, reviewer_name, bookings FROM (SELECT city, reviewer_id, reviewer_name, bookings, dense_rank() OVER (PARTITION BY city ORDER BY bookings DESC) as rank FROM joined GROUP BY city, reviewer_id, reviewer_name, bookings) tmp WHERE rank <= 3 GROUP BY city, reviewer_id, reviewer_name, bookings")

        #save to file
        self.save_toFile(results, "5a.csv")

    #GET TOP THREE HOSTS WITH HIGHEST INCOME FOR EACH CITY(4c)
    '''
    For each city, find top 3 hosts with the highest income (throughout
    the whole time of the dataset). Calculate the estimated income based
    on the listing price and number of days it was booked according to
    the calendar dataset
    '''
    def get_topThree_highestIncome(self):
        df_listings = self.get_df_listings()

        #testing new filter
        #df_listings = df_listings.filter($length("price") > 0 || $"price" > 0)
        #df_listings = df_listings.filter(length("price") > 0)
        df_listings = df_listings.filter("price > 0")

        df_calendar = self.get_df_calendar()
        #use only unavailable listings to calculate price
        df_calendar = self.spark.sql("SELECT listing_id, date FROM calendar WHERE available = 'f'")

        #join dataframes
        df_joined = df_calendar.join(df_listings, df_calendar.listing_id == df_listings.id)

        df_joined.createOrReplaceTempView("joined")

        #get top three seattle
        #self.spark.sql("SELECT city, host_id, host_name, SUM(price) AS income FROM joined WHERE city = 'new york' GROUP BY city, host_id, host_name ORDER BY income DESC LIMIT 3").show()
        '''
        output sample = 1:
        +--------+--------+---------+---------+
        |    city| host_id|host_name|   income|
        +--------+--------+---------+---------+
        |new york|59900772|   Helena|5721125.0|
        |new york| 1235070|    Olson|4859514.0|
        |new york| 3906464|      Amy|4069593.0|
        +--------+--------+---------+---------+
        output sample = 0.01
        +--------+--------+---------+------+
        |    city| host_id|host_name|income|
        +--------+--------+---------+------+
        |new york|11273539|    Julia|4500.0|
        |new york|64454893|     Atul|3300.0|
        |new york| 1106592|    Scott|3000.0|
        +--------+--------+---------+------+
        '''

        #self.spark.sql("SELECT city, host_id, host_name, SUM(price) AS income FROM joined WHERE city = 'new york' GROUP BY city, host_id, host_name ORDER BY income DESC LIMIT 3").show()
        #self.spark.sql("SELECT city, host_id, host_name, SUM(price) AS income FROM (SELECT city, host_id, host_name, price, dense_rank() OVER (PARTITION BY city ORDER BY SUM(price) DESC) as rank FROM joined GROUP BY city, host_id, host_name, price) tmp WHERE rank <= 3 GROUP BY city, host_id, host_name").show()

        #this works, DONE!
        #calculate income
        df_joined = self.spark.sql("SELECT city, host_id, host_name, SUM(price) AS income FROM joined GROUP BY city, host_id, host_name ORDER BY income DESC")

        df_joined.createOrReplaceTempView("joined")
        results = self.spark.sql("SELECT city, host_id, host_name, income FROM (SELECT city, host_id, host_name, income, dense_rank() OVER (PARTITION BY city ORDER BY income DESC) as rank FROM joined GROUP BY city, host_id, host_name, income) tmp WHERE rank <= 3 GROUP BY city, host_id, host_name, income ORDER BY city ASC")

        #save to file
        self.save_toFile(results, "4c.csv")

        return

        #sql
        #self.spark.sql("SELECT city, host_id, host_name, SUM(price) AS income FROM (SELECT city, host_id, host_name, price, dense_rank() OVER (PARTITION BY city ORDER BY SUM(price) DESC) as rank FROM joined GROUP BY city, host_id, host_name, price) tmp WHERE rank <= 3 GROUP BY city, host_id, host_name").show()
        #self.spark.sql("SELECT city, host_id, host_name, SUM(price) FROM (SELECT city, host_id, host_name, price, dense_rank() OVER (PARTITION BY city ORDER BY SUM(price) DESC) as rank FROM joined GROUP BY city, host_id, host_name, price) tmp WHERE rank <= 3 GROUP BY city, host_id, host_name").show()

        #top_three = self.spark.sql("SELECT city, price FROM (SELECT city, price, dense_rank() OVER (PARTITION BY city ORDER BY price DESC) AS rank FROM listings) tmp WHERE rank <= 2")



        #sql
        #self.spark.sql("SELECT city, SUM(PRICE), rn FROM (SELECT city, SUM(PRICE, ROW_NUMBER() OVER (PARTITION BY city ORDER BY SUM(PRICE) DESC) AS rn FROM joined) tmp WHERE rn <= 3 ORDER BY city, rn").show()
        #return
        #get city names
        #city_names_list = self.spark.sql("SELECT DISTINCT city FROM joined").collect()
        '''
        print city_names_list
        for name in city_names_list:
            print type(name)
            print name[0]
        '''

        #df = self.get_df_listings()
        #df.select("price").show()
        #self.spark.sql("SELECT price FROM listings ORDER BY price DESC").show()
        #top_three = self.spark.sql("SELECT city, price FROM (SELECT city, price, dense_rank() OVER (PARTITION BY city ORDER BY price DESC) AS rank FROM listings) tmp WHERE rank <= 2")

        #return top three highest prices for each city (WORKING)
        #top_three = self.spark.sql("SELECT city, price FROM (SELECT city, price, dense_rank() OVER (PARTITION BY city ORDER BY price DESC) AS rank FROM listings) tmp WHERE rank <= 2")
        #remove duplicates
        #top_three = top_three.drop_duplicates()
        #top_three.show()

        #calc_price = udf(lambda x: (float(x)/0.7)*3)
        #df = df.withColumn("price", calc_price("price"))

        #window = Window.partitionBy(df['city']).orderBy(df['price'].desc())
        #top_three = df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3)
        #top_three = top_three.select("city", "price").drop_duplicates()
        #top_three.show()
        return

        #test: calculate top three of one city
        self.spark.sql("SELECT TOP(3) SUM(price) AS income, city, host_id, host_name FROM joined  GROUP BY city, host_id, host_name ORDER BY income DESC").show()
        #self.spark.sql("SELECT city, host_id, host_name, SUM(price) AS income FROM joined WHERE city = 'new york' GROUP BY city, host_id, host_name ORDER BY income DESC LIMIT 3").show()
        return

        query_generic = "'SELECT city, host_id, host_name, SUM(price) AS income FROM joined WHERE city = city_name GROUP BY city, host_id, host_name ORDER BY income DESC'"
        for city_name in city_names_list:
            city_name = "'new york'"
            query = query_generic.replace("city_name", str(city_name))
            #top_three = self.spark.sql("SELECT city, host_id, host_name, SUM(price) AS income FROM joined WHERE city = @city_name[0] GROUP BY city, host_id, host_name ORDER BY income DESC LIMIT 3").show()
            #print top_three
            top_three = self.spark.sql(eval(query))
            top_three.show()
            return
        return

        #sql
        df_joined.createOrReplaceTempView("joined")
        self.spark.sql("SELECT city, host_id, host_name, SUM(price) AS income, COUNT(*) num FROM joined GROUP BY city, host_id, host_name HAVING COUNT(*) <= 3 ORDER BY income DESC LIMIT 9").show()


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
        #df2 = df
        #df2.createOrReplaceTempView("df_2")


        #test
        #self.spark.sql("SELECT SUM(part)/SUM(total) FROM (SELECT host_id, total, COUNT(DISTINCT host_id) AS part FROM (SELECT host_id, host_listings_count, COUNT(DISTINCT host_id) AS total FROM listings GROUP BY host_id, host_listings_count) WHERE host_listings_count > 1 GROUP BY host_id, total) GROUP BY host_id").show()
        #self.spark.sql("SELECT host_id, part, total FROM (SELECT host_id, total, COUNT(DISTINCT host_id) AS part FROM (SELECT host_id, host_listings_count, COUNT(DISTINCT host_id) AS total FROM listings GROUP BY host_id, host_listings_count) WHERE host_listings_count > 1 GROUP BY host_id, total) GROUP BY host_id, part, total").show()


        total_hosts = self.spark.sql("SELECT COUNT(DISTINCT host_id) AS total FROM listings")
        hosts_moreThanOne = self.spark.sql("SELECT COUNT(DISTINCT host_id) AS part FROM listings WHERE host_listings_count > 1")

        df_joined = total_hosts.join(hosts_moreThanOne, total_hosts.total != hosts_moreThanOne.part)
        df_joined.createOrReplaceTempView("joined")

        results = self.spark.sql("SELECT ROUND((part/total)*100, 2) AS percentage FROM joined")

        #save to file
        self.save_toFile(results, "4b.csv")



    #GET ESTIMATED AMOUNT SPENT ON ACCOMODATION PER YEAR FOR EACH CITY(3e)
    def get_amountSpent_accomodationPerYear(self):
        df = self.get_df_listings()
        '''
        df_listings = df_listings.filter(length("price") > 0)
        df_listings = df_listings.filter("price >= 0")
        df_listings = df_listings.filter(length("city") > 0)
        df_reviews = self.get_df_reviews()
        #join dataframes
        df_joined = df_reviews.join(df_listings, df_reviews.listing_id == df_listings.id)

        #change date format to year only
        df_joined = df_joined.withColumn("date", regexp_replace("date", "((-(\d){0,2}-(\d){0,2})$)", ""))
        df_reviews = df_reviews.filter(length("date") > 3)

        #calculate price using assumptions
        calc_price = udf(lambda x: (float(x)/0.7)*3)
        df_joined = df_joined.withColumn("price", calc_price("price"))

        #sql
        df_joined.createOrReplaceTempView("joined")
        '''
        #self.spark.sql("SELECT city, ROUND((SUM(price)/COUNT(DISTINCT date)), 2) AS amountSpent_accommodationPerYear FROM joined GROUP BY city").show()
        self.spark.sql("SELECT city, ROUND((((SUM(reviews_per_month)*12)/0.7)*3)*AVG(PRICE), 2) AS accommodationPerYear FROM listings GROUP BY city").show()

    #GET ESTIMATED NUMBER OF NIGHTS BOOKED PER YEAR FOR EACH CITY(3d)
    def get_numberOf_nightsBooked(self):
        df = self.get_df_listings()
        '''
        df_listings.select("reviews_per_month").show()
        df_listings = df_listings.filter("reviews_per_month > 0")
        df_listings = df_listings.filter(length("city") > 0)
        df_reviews = self.get_df_reviews()
        #join dataframes
        df_joined = df_reviews.join(df_listings, df_reviews.listing_id == df_listings.id)

        #change date format to year only
        df_joined = df_joined.withColumn("date", regexp_replace("date", "((-(\d){0,2}-(\d){0,2})$)", ""))

        #convert to nights booked from reviews
        calc_nights = udf(lambda x: (float(x)/0.7)*3)
        df_joined = df_joined.withColumn("reviews_per_month", calc_nights("reviews_per_month"))

        df_joined.select("reviews_per_month").show()

        #sql
        df_joined.createOrReplaceTempView("joined")
        '''
        #self.spark.sql("SELECT city, ROUND(((SUM(reviews_per_month))/COUNT(DISTINCT date)), 2) AS avg_nightsBooked_perYear FROM joined GROUP BY city ORDER BY city").show()

        self.spark.sql("SELECT city, ROUND(((SUM(reviews_per_month)*12)/0.7)*3, 2) AS nightsBooked_perYear FROM listings GROUP BY city ").show()

    #GET AVERAGE NUMBER OF REVIEWS PER MONTH FOR EACH CITY(3c)
    def get_avg_reviewsPerMonth(self):
        df = self.get_df_listings()
        #df_listings = df_listings.filter(length("city") > 0)
        #df_listings = df_listings.filter(length("reviews_per_month") > 0)
        #df_listings.select("reviews_per_month").show()
        #df_reviews = self.get_df_reviews()

        #join dataframes
        #df_joined = df_reviews.join(df_listings, df_reviews.listing_id == df_listings.id)

        #remove day from date format
        #df_joined = df_joined.withColumn("date", regexp_replace("date", "(-(\d){0,2})$", ""))
        #df_joined.select("date").show()

        #sql
        #df_joined.createOrReplaceTempView("joined")
        #results = self.spark.sql("SELECT city, ROUND(SUM(reviews_per_month)/COUNT(DISTINCT date), 2) AS avg_reviewsPerMonth FROM joined GROUP BY city").show()
        results = self.spark.sql("SELECT city, ROUND(AVG(reviews_per_month)) as avg_reviewsPerMonth FROM listings GROUP BY city")

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

        res_list = []
        #running half of dataframe at a time
        for c in df.columns[len(df.columns)/2:]:
            count = df.agg(countDistinct(c))
            result = count.collect()
            res_list.append(result)
        print(res_list)

    #SAVE DATAFRAME TO FILE
    def save_toFile(self, dataframe, filename):
        dataframe = dataframe.repartition(1)
        dataframe.write.format("csv").option("header", "true").option("delimiter", ",").save(filename)

    #FIND AVERAGE NUMBER OF REVIEWS PER MONTH FOR EACH CITY(3c)

    #HOW TO JOIN DATAFRAMES
    #df3 = df1.join(df2, df1.listing_id == df2.id)

    '''
    #need to join on listing_id to get city from listings

    #date format: 2016-09-15
    #trim whitespaces from dates
    df1 = df1.withColumn("date", trim(df1.date))
    #remove day from date format
    df1 = df1.withColumn("date", regexp_replace("date", "(-(\d){0,2}$)", ""))
    #new date format: 2016-09
    #need to join with df from listings
    data = sc.textFile("listings_us.csv")
    schemaString = data.first()
    #set data sample size
    #data = data.sample(False, 0.01, 7)
    #split into parts
    parts = data.map(lambda l: l.split("\t"))
    #create string for mapping
    col_size = len(schemaString.split()) #95 as index <- OK!
    map_string = ""
    for i in range(col_size):
        map_string += "p[%d]" % i
        if i + 1 < col_size:
            map_string += ", "
    details = parts.map(lambda p: (eval(map_string)))
    #details = parts.map(lambda p: (map_string))
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    df2 = spark.createDataFrame(details, schema)
    df2.printSchema()
    #avoid duplicates
    #trim whitespaces
    df2 = df2.withColumn("city", trim(df2.city))
    #make lowercase
    df2 = df2.withColumn("city", lower(df2.city))
    #remove bad city names
    df2 = df2.filter(length("city") > 1)


    #sql
    df3.createOrReplaceTempView("reviews_and_listings")
    '''
    #df4 = spark.sql("SELECT city, date, count(listing_id) from reviews_and_listings GROUP BY city, date ORDER BY city ASC, date ASC")
    #results = spark.sql("SELECT city, count(listing_id)/count(DISTINCT date) AS average_reviews_per_month FROM reviews_and_listings GROUP BY city ORDER BY city ASC").collect()
    '''
    for line in results:
        print line
    '''


'''
def distinct_count_listings():
    #import file
    data = sc.textFile("listings_us.csv")
    #use first line as header
    schemaString = data.first()
    #set sample
    data = data.sample(False, 0.001, 7)
    #split into parts
    parts = data.map(lambda l: l.split())
    #string_for_strip = ""
    #print(parts.count())
    #for i in range(parts.count()):
    #    string_for_strip += "p[%d].strip(), " % i
    #    if i != parts.count():
    #        string_for_strip += ", "
    #print string_for_strip

    #split into columns
    #details = parts.map(lambda p: (string_for_strip))
    details = parts.map(strip_listings())

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    df = spark.createDataFrame(details, schema)
    #filter out incomplete data
    #df = df.filter("listing_id > 0")
    df.printSchema()
    df.show()
    df.createOrReplaceTempView("listings")
    count = df.agg(countDistinct("access"))
    #count = df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns))
    #count.show()
    return
'''
'''
def distinct_count_calendar():
    #import file
    data = sc.textFile("calendar_us.csv")
    #set sample
    data = data.sample(False, 0.1, 7)
    #split into parts
    parts = data.map(lambda l: l.split())
    #split into three columns
    details = parts.map(lambda p: (p[0], p[1].strip(), p[2].strip()))
    #use first line as header
    schemaString = data.first()
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    df = spark.createDataFrame(details, schema)
    #filter out incomplete data
    df = df.filter("listing_id > 0")
    df.printSchema()
    #df.show()
    df.createOrReplaceTempView("calendar")
    count = df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns))
    count.show()
'''

if __name__ == "__main__":
    airbnb = airbnb()
    airbnb.get_neighborhoodComparison()
