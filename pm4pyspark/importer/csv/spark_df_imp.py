import datetime
import pyspark
import pyspark.sql.functions as F

from dateutil import parser, tz
from pm4py.objects.log import log as log_instance
from pm4py.objects.conversion.log.versions import to_event_log
from pm4pyspark.importer.constants import DEFAULT_NUM_PARTITION
from pyspark.sql import SparkSession
from pyspark.sql.types import *




def convert_timestamp_to_utc_in_df(df, timest_columns=None):
    """Converts datatype of column "time:timestamp" from `StringType` to `TimestampType` as UTC timezone
    """

    if timest_columns is None:
        timest_columns = {"time:timestamp"}
    for col in timest_columns:
        if df.schema[col].dataType == StringType():
            utc_zone =  tz.gettz("UTC")
            func = F.udf(lambda x: parser.parse(x).astimezone(utc_zone).isoformat(timespec='milliseconds'), StringType())
            func2 = F.udf(lambda x: datetime.datetime.strptime(''.join(x[:-6].rsplit(':', 0)), '%Y-%m-%dT%H:%M:%S.%f'), TimestampType())
            #df = df.withColumn(col + "_utc", func2(func(df[col])))
            #df = df.drop(col).withColumnRenamed(col + "_utc", col)
            df = df.withColumn(col, func2(func(df[col])))

    return df


def import_sparkdf_from_path_wo_timeconversion(path, sep=None, quote=None, header=None, inferSchema=None, numPartition=DEFAULT_NUM_PARTITION):
    """Imports a Spark DataFrame from the given path of CSV format file (without time conversion)
    """
    ''
    spark = (SparkSession.
             builder.
             master('local[*]').
             config('spark.sql.shuffle.partitions', numPartition).
             getOrCreate())

    spark_df = spark.read.csv(path, sep=sep, quote=quote, header=header, inferSchema=inferSchema)

    return spark_df


def convert_caseid_column_to_str(df, case_id_glue="case:concept:name"):
    """Converts Case ID column to StringType
    """
    df = df.withColumn(case_id_glue, df[case_id_glue].cast(StringType()))

    return df



def import_sparkdf_from_path(path, sep=None, quote=None, header=None, inferSchema=None, timest_columns=None,
                             sort=False, sort_field="time:timestamp", ascending=True, numPartition=DEFAULT_NUM_PARTITION):
    """Imports a Spark DataFrame from the given path of CSV format file (with time conversion)
    """

    spark_df = import_sparkdf_from_path_wo_timeconversion(path, sep=sep, quote=quote, header=header,
                                                          inferSchema=inferSchema, numPartition=numPartition)
    spark_df = convert_timestamp_to_utc_in_df(spark_df, timest_columns=timest_columns)

    if sort and sort_field:
        if ascending is True:
            spark_df = spark_df.orderBy(sort_field)
        else:
            spark_df = spark_df.orderBy(sort_field, ascending=False)

    return spark_df


def import_event_stream(path, sep=None, quote=None, header=None, inferSchema=True, timest_columns=None, sort=True,
                        sort_field="time:timestamp", ascending=True, numPartition=DEFAULT_NUM_PARTITION):
    """Imports an `EventStream` from the given path of CSV format file
    """

    spark_df = import_sparkdf_from_path(path, sep=sep, quote=quote, header=header, inferSchema=inferSchema,
                                        timest_columns=timest_columns, sort=sort, sort_field=sort_field,
                                        ascending=ascending, numPartition=numPartition)
    rdd = spark_df.rdd.map(lambda row: row.asDict())
    event_stream = rdd.collect()
    event_stream = log_instance.EventStream(event_stream, attributes={'origin': 'csv'})
    #pair_rdd = rdd.map(lambda s: (s[0], (s[1], s[2])))
    #pair_rdd_group = pair_rdd.groupByKey().mapVal0ues(list)
    #return pair_rdd_group.collect()
    return event_stream


def transform_event_stream_to_event_log(event_stream, include_case_attributes=True, enable_deepcopy=False):
    """Transforms an `EventStream` to an `EventLog`
    """

    log = to_event_log.transform_event_stream_to_event_log(event_stream,
                                                           include_case_attributes=include_case_attributes,
                                                           enable_deepcopy=enable_deepcopy)

    return log
