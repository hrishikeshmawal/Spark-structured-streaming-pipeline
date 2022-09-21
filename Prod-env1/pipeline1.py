import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os



def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df



os.chdir("..")
print(os.path.abspath(os.curdir))


def main():
    os.environ['HADOOP_HOME'] = "C:\\hadoop"
    print(os.environ['HADOOP_HOME'])

    spark= SparkSession.builder.appName("spark project").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    
    #Here the schema of the incoming streaming data is defined
    structureSchema = StructType([
        StructField('datetime', StringType(), True),
        StructField('sales', StructType([
             StructField('quantity', LongType(), True),
             StructField('total_price', DoubleType(), True)
             ])),
        StructField('analytics', StructType([
             StructField('clicks', LongType(), True),
             StructField('impressions', LongType(), True)
             ]))        
     ])



   #reading the stream 
   stream_df = spark\
                .readStream\
                .format("json") \
                .schema(structureSchema) \
                .option("path","data/json_files")\
                .load()
    print("after reading")



    print(stream_df.isStreaming)
    print(stream_df.printSchema())

    print("after showing schema")
    print(os.path.abspath(os.curdir)+"/data/csv_files")
      
    #flattening the json  
    df_flatten= flatten(stream_df)
   
    #writing the stream to csv files
    stream_df_query = df_flatten\
                         .writeStream.format("csv")\
                             .option("header","true")\
                                  .option("format", "append")
                                      .option("path","data/csv_files")
                                         .option("checkpointLocation", "data/csv_files/checkpoint_path")\
                                              .outputMode("append").start()
   #  stream_df_query = df_flatten.writeStream.outputMode("append").format("console").option("checkpointLocation", "streaming-checkpoint-loc-json").trigger(processingTime="10 second").start()


    stream_df_query.awaitTermination()




    
      

if __name__ == "__main__":
    print("Application Started ...")

   

    main()


    print("Streaming Application Completed.")

    # stream_df_query = streamingInputDF \
    #                     .writeStream \
    #                     .format("console") \
    #                     .start()                  


    # stream_df_query.awaitTermination()

    # print("Application Completed.")
