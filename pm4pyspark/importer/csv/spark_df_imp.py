import pyspark.sql.functions as F

from dateutil import parser, tz
from pm4py.objects.log import log as log_instance
from pm4py.objects.conversion.log.versions import to_event_log
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def convert_timestamp_to_utc_in_df(df, timest_columns=None):
    '''
    Converts datatype of column "time:timestamp" from StringType to TimestampType as UTC timezone
    '''

    if timest_columns is None:
        timest_columns = {"time:timestamp"}
    for col in timest_columns:
        if df.schema[col].dataType == StringType():
            utc_zone =  tz.gettz("UTC")
            func = F.udf(lambda x: parser.parse(x).astimezone(utc_zone).isoformat(), StringType())
            df = df.withColumn(col + "_utc", F.to_timestamp(func(df[col]), "yyyy-MM-dd'T'HH:mm:ss"))
            df = df.drop(col).withColumnRenamed(col + "_utc", col)

    return df

def import_sparkdf_from_path_wo_timeconversion(path, sep=None, quote=None, header=None, inferSchema=None):
    '''
    Imports a Spark DataFrame from the given path of CSV format file (without time conversion)
    '''

    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.read.csv(path, sep=sep, quote=quote, header=header, inferSchema=inferSchema)

    return spark_df

def import_sparkdf_from_path(path, sep=None, quote=None, header=None, inferSchema=None, timest_columns=None, sort=False, sort_field="time:timestamp", ascending=True):
    '''
    Imports a Spark DataFrame from the given path of CSV format file (with time conversion)
    '''

    spark_df = import_sparkdf_from_path_wo_timeconversion(path, sep=sep, quote=quote, header=header, inferSchema=inferSchema)
    spark_df = convert_timestamp_to_utc_in_df(spark_df, timest_columns=timest_columns)

    if sort and sort_field:
        if ascending is True:
            spark_df = spark_df.orderBy("time:timestamp")
        else:
            spark_df = spark_df.orderBy("time:timestamp", ascending=False)

    return spark_df

def import_event_stream(path, parameters=None):
    '''
    Imports an EventStream from the given path of CSV format file
    '''

    sep = None
    quote = None
    header = None
    inferSchema = True
    timest_columns = None
    sort = True
    sort_field="time:timestamp"
    ascending=True

    if parameters is None:
        parameters = {}
    if "sep" in parameters:
        sep = parameters["sep"]
    if "quote" in parameters:
        quote = parameters["quote"]
    if "header" in parameters:
        header = parameters["header"]
    if "inferSchema" in parameters:
        inferSchema = parameters["inferSchema"]
    if "timest_columns" in parameters:
        timest_columns = parameters["timest_columns"]
    if "sort" in parameters:
        sort = parameters["sort"]
    if "sort_field" in parameters:
        sort_field = parameters["sort_field"]
    if "ascending" in parameters:
        ascending = parameters["ascending"]

    spark_df = import_sparkdf_from_path(path, sep=sep, quote=quote, header=header, inferSchema=inferSchema, timest_columns=timest_columns, sort=sort, sort_field=sort_field, ascending=ascending)

    rdd = spark_df.rdd.map(lambda row: row.asDict())
    event_stream = rdd.collect()
    event_stream = log_instance.EventStream(event_stream, attributes={'origin': 'csv'})
    #pair_rdd = rdd.map(lambda s: (s[0], (s[1], s[2])))
    #pair_rdd_group = pair_rdd.groupByKey().mapVal0ues(list)
    #return pair_rdd_group.collect()
    return event_stream

def transform_event_stream_to_event_log(event_stream, include_case_attributes=True, enable_deepcopy=False):
    '''
    Transforms an EventStream to an EventLog
    '''

    log = to_event_log.transform_event_stream_to_event_log(event_stream, include_case_attributes=include_case_attributes, enable_deepcopy=enable_deepcopy)

    return log
