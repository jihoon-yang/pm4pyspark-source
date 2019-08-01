import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import *




def export_sparkdf(df, path, case_id_key="case:concept:name", mode=None, partitionBy="@@partitioning", compression=None, num_partitions=128):

    get_hash = F.udf(lambda x: abs(hash(x)) % num_partitions, LongType())
    spark = (SparkSession.
             builder.
             master('local[*]').
             config('spark.executor.memory', '5gb').
             config("spark.cores.max", "6").
             getOrCreate())

    #get_hash_udf = F.udf(get_hash, LongType())

    df = df.withColumn(partitionBy, get_hash(case_id_key))
    for c in df.columns:
        df = df.withColumnRenamed(c, c.replace(':', 'AAA'))

    df.write.parquet(path, mode=mode, partitionBy=partitionBy, compression=compression)
