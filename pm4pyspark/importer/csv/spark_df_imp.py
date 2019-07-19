from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from dateutil import parser, tz
from pyspark.sql.types import *

def convert_timestamp_to_utc_in_df(df, timest_columns=None):
    if timest_columns is None:
        timest_columns = {"time:timestamp"}
    for col in timest_columns:
        if df.schema[col].dataType == StringType():
            utc_zone =  tz.gettz("UTC")
            func = F.udf(lambda x: parser.parse(x).astimezone(utc_zone).isoformat(), StringType())
            df = df.withColumn("time_utc", F.to_timestamp(func(df[col]), "yyyy-MM-dd'T'HH:mm:ss"))
            df = df.drop(col).withColumnRenamed("time_utc", col)

    return df

def import_sparkdf_from_path_wo_timeconversion(path, sep=None, quote=None, header=None, inferSchema=None):
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.read.csv(path, sep=sep, quote=quote, header=header, inferSchema=inferSchema)

    return spark_df

def import_sparkdf_from_path(path, sep=None, quote=None, header=None, inferSchema=None, timest_columns=None, sort=False, sort_field="time:timestamp", ascending=True):
    """
    Imports a Spark Dataframe from the given path of csv format file
    (with selecting the most important columns:
        "case:concept:name"
        "concept:name"
        "time:timestamp"
    and order by timestamp)

    Parameters
    ----------
    path:
        Input CSV file path
    sep:
        column separator
    quote:
        sets a single character used for escaping quoted values where the
        separator can be part of the value. If None is set, it uses the default
        value, ``"``. If an empty string is set, it uses ``u0000`` (null character).
    header:
        uses the first line as names of columns. If None is set, it uses the
        default value, ``False``.
    inferSchema:
        infers the input schema automatically from data. It requires one extra
        pass over the data. If None is set, it uses the default value, ``False``.

    Returns
    -------
    spark_df
        Spark Dataframe
    """
    spark_df = import_sparkdf_from_path_wo_timeconversion(path, sep=sep, quote=quote, header=header, inferSchema=inferSchema)
    spark_df = convert_timestamp_to_utc_in_df(spark_df, timest_columns=timest_columns)

    if sort and sort_field:
        if ascending is True:
            spark_df = spark_df.orderBy("time:timestamp")
        else:
            spark_df = spark_df.orderBy("time:timestamp", ascending=False)

    return spark_df
