from pyspark.sql import SparkSession

def import_csv(path):
    spark_session = SparkSession.builder.getOrCreate()
    spark_df = spark_session.read.csv(path, header=True)
    filtered_spark_df = spark_df.select('case:concept:name', 'concept:name', 'time:timestamp')

    return filtered_spark_df
