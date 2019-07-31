from pm4py.objects.log import log as log_instance
from pm4py.objects.conversion.log.versions import to_event_log
from pm4pyspark.importer.constants import DEFAULT_NUM_PARTITION
from pyspark.sql import SparkSession


def apply(path, parameters=None):
    """Imports a Parquet file
    """

    if parameters is None:
        parameters = {}

    numPartition = parameters["numPartition"] if "numPartition" in parameters else DEFAULT_NUM_PARTITION

    spark = (SparkSession.
             builder.
             master('local[*]').
             config('spark.sql.shuffle.partitions', numPartition).
             getOrCreate())

    spark_df = spark.read.parquet(path)
    for c in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(c, c.replace('AAA', ':'))

    return spark_df


def import_sparkdf_from_path(path, sort=False, sort_field="time:timestamp", ascending=True, numPartition=DEFAULT_NUM_PARTITION):
    """Imports a Spark DataFrame from the given path of PARQUET format file
    """

    parameters = {}
    parameters["numPartition"] = numPartition

    spark_df = apply(path, parameters=parameters)

    if sort and sort_field:
        if ascending is True:
            spark_df = spark_df.orderBy(sort_field)
        else:
            spark_df = spark_df.orderBy(sort_field, ascending=False)

    return spark_df


def import_event_stream(path, sort=True, sort_field="time:timestamp", ascending=True, numPartition=DEFAULT_NUM_PARTITION):
    """Imports an `EventStream` from the given path of PARQUET format file
    """

    spark_df = import_sparkdf_from_path(path, sort=sort, sort_field=sort_field, ascending=ascending, numPartition=numPartition)
    rdd = spark_df.rdd.map(lambda row: row.asDict())
    event_stream = rdd.collect()
    event_stream = log_instance.EventStream(event_stream, attributes={'origin': 'parquet'})
    return event_stream


def transform_event_stream_to_event_log(event_stream, include_case_attributes=True, enable_deepcopy=False):
    """Transforms an `EventStream` to an `EventLog`
    """

    log = to_event_log.transform_event_stream_to_event_log(event_stream,
                                                           include_case_attributes=include_case_attributes,
                                                           enable_deepcopy=enable_deepcopy)

    return log
