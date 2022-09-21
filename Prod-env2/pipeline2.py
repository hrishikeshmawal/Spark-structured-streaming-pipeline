import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import os




os.chdir("..")
print(os.path.abspath(os.curdir))

def main():
    os.environ['HADOOP_HOME'] = "C:\\hadoop"
    print(os.environ['HADOOP_HOME'])

    spark= SparkSession.builder.appName("spark project").getOrCreate()

    #defining the schema of the incoming sreaming data
    structureSchema = StructType([ StructField('datetime', TimestampType(), True), 
                         StructField('sales_quantity', LongType(), True),
                         StructField('sales_total_price', DoubleType(), True),
                         StructField('analytics_clicks', LongType(), True),
                         StructField('analytics_impressions', LongType(), True) ])



    #reading the stream from pipeline1 output
    stream_df = spark \
                .readStream \
                .format("csv") \
                .option("header", "true").option("maxFilesPerTrigger",1) \
                .schema(structureSchema) \
                .load(path="data/csv_files")


    print("after reading")


    print(stream_df.isStreaming)
    print(stream_df.printSchema())

    print("after showing schema")



    # window aggregations on the stream 
    stream_count_df =stream_df.withWatermark("datetime", "2 minutes") \
                                   .groupBy(window("datetime",  "1 minutes")) \
                                      .agg({"sales_quantity":"sum","sales_total_price":"sum","analytics_clicks":"sum","analytics_impressions":"sum"})   

    #writing the stream and checking on the console
    stream_df_query = stream_count_df.writeStream.outputMode("complete").format("console").option("checkpointLocation", "data/parquet_files/checkpoint_path").start()
    
   
   
    stream_df_query.awaitTermination()




    
      

if __name__ == "__main__":
    print("Application Started ...")

   
    main()


    print("Streaming Application Completed.")

    
