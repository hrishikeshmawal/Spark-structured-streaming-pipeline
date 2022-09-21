# Spark-structured-streaming-pipeline

In this project a streaming pipeline is developed that listens to some data sources and dumps the data into storage. 

We have deployed the pipeline into two different environments (Prod-env1 and Prod-env2). 


For Prod-env1, the following is expected:

1.The pipeline reads JSON files as a stream from data directory. The JSON file will be generated once the main.py file is run.
2.It processes and flattens the data.
3.It stores it as CSV files in data directory.


And for Prod-env2:

1.The pipeline reads the stored CSV files from the previous step.
2.Applies the corresponding transformations.
3.And stores the data in Parquet files.
