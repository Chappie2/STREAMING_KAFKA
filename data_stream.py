import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import findspark

# TODO Create a schema for incoming resources
schema = StructType([
       StructField("crime_id", StringType(), True),
       StructField("original_crime_type_name", StringType(), True),
       StructField("report_date", TimestampType(), True),
       StructField("call_date", TimestampType(), True),
       StructField("offense_date", TimestampType(), True),
       StructField("call_time", StringType(), True),
       StructField("call_date_time", TimestampType(), True),
       StructField("disposition", StringType(), True),
       StructField("address", StringType(), True),
       StructField("city", StringType(), True),
       StructField("state", StringType(), True),
       StructField("agency_id", StringType(), True),
       StructField("address_type", StringType(), True),
       StructField("common_location", StringType(), True)
])

def run_spark_job(spark):
    # set up correct bootstrap server and port
    spark.sparkContext.setLogLevel("WARN")
    
    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    # Subscribe to 1 topic
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "sfpd.service.calls") \
      .option("startingOffsets", "earliest") \
      .option("maxRatePerPartition", 20) \
      .option("maxOffsetPerTrigger", 200) \
      .option("stopGracefullyOnShutdown", "true") \
      .load()
    
    df.printSchema()
    
    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table \
        .withWatermark("call_date_time", "20 seconds").select("original_crime_type_name","disposition").distinct()

    # count the number of original crime type
    agg_df = distinct_table \
        .agg(psf.count('original_crime_type_name').alias('count'))
    
    logger.info("Streaming count of crime types")
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query_1 = agg_df \
        .writeStream \
        .format('console') \
        .outputMode('Complete') \
        .start()
    
    # TODO attach a ProgressReporter
    query_1.awaitTermination()
    
    logger.debug("Reading static data from disk")
    # TODO get the right radio code json path
    file_name = 'radio_code.json'
    radio_df = spark.read.json(file_name, multiLine=True)
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_df.withColumnRenamed("disposition_code", "disposition")
    
    logger.info("Streaming crime types and descriptions")
    # TODO join on disposition column
    joined_df = agg_df\
    .join(radio_code_df, col('agg_df.disposition') == col('radio_df.disposition'), 'left_outer')
    
    query_2 = joined_df \
        .writeStream \
        .format('console') \
        .outputMode('Complete') \
        .start()
    
    query_2.awaitTermination()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    
    findspark.init()    
    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port",3000) \
        .config("spark.driver.memory","2g") \
        .config("spark.executor.memory","2g") \
        .appName("ServiceCalls") \
        .getOrCreate()
    

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()