import json

from google.cloud import storage
from mongodb import *
from pyspark.sql import SparkSession
from pyspark import SparkConf

from user_definition import *


def return_static_amazon_json(service_account_key_file,
                bucket_name,
                blob_name):
    
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    json_lines = blob.download_as_string().decode().splitlines()
    return json_lines


def return_json(service_account_key_file,
                bucket_name,
                blob_name):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    json_str = blob.download_as_string().decode("utf8")
    json_data = json.loads(json_str)
    return json_data


def insert_aggregates_to_mongo():
    # Spark Configuration Stuff
    import os

    spark_conf = SparkConf()
    spark_conf.setAppName("MySparkJob")
    spark_conf.set("spark.executor.memory", "2g")
    spark_conf.set("spark.executor.cores", "2")
    spark_conf.set("spark.driver.memory", "2g")
    spark_conf.set("spark.driver.maxResultSize", "2g")
    spark_conf.set("spark.yarn.queue", "default")
    spark_conf.set("spark.default.parallelism", "8")

    # Create a SparkSession
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    # spark = SparkSession.builder.getOrCreate()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("google.cloud.auth.service.account.json.keyfile", service_account_key_file)
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

    # Get the data from GCP as a list of strings in json format
    static_json = return_static_amazon_json(service_account_key_file,
                                            bucket_name,
                                            blob_name_static)

    # Format the static data using spark
    # static_rdd = spark.parallelize(static_json)
    static_rdd = spark.sparkContext.parallelize(static_json)

    static_rdd = static_rdd.map(lambda x: json.loads(x))

    static_rdd = static_rdd.map(lambda x:
                                    (x['asin'],
                                           {'review': x['reviewText'] if 'reviewText' in x else "",
                                            'rating': x['overall'] if 'overall' in x else "",
                                            'title': x['summary'] if 'summary' in x else ""}))
    static_rdd = static_rdd.groupByKey().map(lambda x: (x[0], x[1].data))
    
    # Get the API data from GCP

    # Format the API data using spark so it has key=ASIN #, value=list of reviews
    # api_rdd = spark.sparkContext.parallelize([return_json(service_account_key_file, bucket_name, blob_name_static)])
    api_rdd = spark.sparkContext.parallelize([return_json(service_account_key_file, bucket_name, blob_name_api)])
    # Combine API data and static data
   
    # Aggregate static and API data and format into dictionary
    items = static_rdd.aggregateByKey([], agg_reviews_seq, agg_reviews_comb)
    aggregates = items.groupByKey().map(lambda x:
                                           {"ASIN": x[0], 
                                            "reviews": x[1].data[0]})
    aggregates = aggregates.union(api_rdd)


    mongodb = MongoDBCollection(mongo_username,
                                mongo_password,
                                mongo_ip_address,
                                database_name,
                                collection_name)


    # Insert the formatted data into MongoDB
    for aggregate in aggregates.collect():
        mongodb.insert_one(aggregate)

    spark.stop()


def agg_reviews_seq(a,b):
    a.extend(b)
    return a

def agg_reviews_comb(a,b):
    a.extend(b)
    return a

if __name__=="__main__":
    insert_aggregates_to_mongo()