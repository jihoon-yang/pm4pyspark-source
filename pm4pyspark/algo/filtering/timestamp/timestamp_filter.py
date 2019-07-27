import pyspark.sql.functions as F

from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME
from pm4py.algo.filtering.common.timestamp.timestamp_common import get_dt_from_string
from pm4py.util.constants import PARAMETER_CONSTANT_TIMESTAMP_KEY, PARAMETER_CONSTANT_CASEID_KEY
from pm4py.objects.log.util.xes import DEFAULT_TIMESTAMP_KEY
from pm4pyspark.importer.csv import spark_df_imp as importer
from pyspark.sql.window import Window




def filter_traces_contained(df, dt1, dt2, parameters=None):
    """Gets traces that are contained in the given interval
    """

    if parameters is None:
        parameters = {}
    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    dt1 = get_dt_from_string(dt1)
    dt2 = get_dt_from_string(dt2)
    df_converted = importer.convert_timestamp_to_utc_in_df(df, timest_columns={timestamp_key})
    df_ordered = df_converted.orderBy(case_id_glue, timestamp_key)
    w = Window().partitionBy(case_id_glue).orderBy(timestamp_key)
    w2 = Window().partitionBy(case_id_glue).orderBy(F.desc(timestamp_key))
    stacked = df_ordered.withColumn(timestamp_key + "_last", F.max(df_ordered[timestamp_key]).over(w2))
    stacked = stacked.withColumn(timestamp_key + "_first", F.min(stacked[timestamp_key]).over(w))
    stacked = stacked.filter(stacked[timestamp_key + "_first"] > dt1)
    stacked = stacked.filter(stacked[timestamp_key + "_last"] < dt2)
    stacked_dropped = stacked.drop(timestamp_key + "_last", timestamp_key + "_first")

    return stacked_dropped


def filter_traces_intersecting(df, dt1, dt2, parameters=None):
    """Filters traces intersecting the given interval
    """

    if parameters is None:
        parameters = {}
    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    dt1 = get_dt_from_string(dt1)
    dt2 = get_dt_from_string(dt2)
    df_converted = importer.convert_timestamp_to_utc_in_df(df, timest_columns={timestamp_key})
    df_ordered = df_converted.orderBy(case_id_glue, timestamp_key)
    w = Window().partitionBy(case_id_glue).orderBy(timestamp_key)
    w2 = Window().partitionBy(case_id_glue).orderBy(F.desc(timestamp_key))
    stacked = df_ordered.withColumn(timestamp_key + "_last", F.max(df_ordered[timestamp_key]).over(w2))
    stacked = stacked.withColumn(timestamp_key + "_first", F.min(stacked[timestamp_key]).over(w))

    stacked.cache()
    #stacked1 = stacked.filter(stacked[timestamp_key + "_first"] > dt1)
    #stacked1 = stacked1.filter(stacked1[timestamp_key + "_first"] < dt2)
    stacked1 = stacked.filter(stacked[timestamp_key + "_first"].between(dt1, dt2))
    #stacked2 = stacked.filter(stacked[timestamp_key + "_last"] > dt1)
    #stacked2 = stacked2.filter(stacked2[timestamp_key + "_last"] < dt2)
    stacked2 = stacked.filter(stacked[timestamp_key + "_last"].between(dt1, dt2))
    stacked3 = stacked.filter(stacked[timestamp_key + "_first"] < dt1)
    stacked3 = stacked3.filter(stacked3[timestamp_key + "_last"] > dt2)
    stacked.unpersist()

    stacked = stacked1.union(stacked2)
    stacked = stacked.union(stacked3)
    stacked = stacked.drop(timestamp_key + "_last", timestamp_key + "_first")\
                     .distinct().orderBy(case_id_glue, timestamp_key)

    return stacked


def apply_events(df, dt1, dt2, parameters=None):
    """Gets a new Spark DataFrame with all the events contained in the given interval
    """

    if parameters is None:
        parameters = {}
    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    dt1 = get_dt_from_string(dt1)
    dt2 = get_dt_from_string(dt2)
    df = df.filter(df[timestamp_key] > dt1)
    df = df.filter(df[timestamp_key] < dt2)

    return df
