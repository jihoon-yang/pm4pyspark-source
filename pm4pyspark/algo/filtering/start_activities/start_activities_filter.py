import pyspark.sql.functions as F

from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME, DECREASING_FACTOR
from pm4py.algo.filtering.common.start_activities import start_activities_common
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_TIMESTAMP_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_ACTIVITY_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_CASEID_KEY
from pm4py.util.constants import GROUPED_DATAFRAME
from pyspark.sql.window import Window




def apply(df, values, parameters=None):
    """Filters the Spark dataframe on start activities
    """

    if parameters is None:
        parameters = {}

    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    activity_key = parameters[
        PARAMETER_CONSTANT_ACTIVITY_KEY] if PARAMETER_CONSTANT_ACTIVITY_KEY in parameters else DEFAULT_NAME_KEY
    grouped_df = parameters[GROUPED_DATAFRAME] if GROUPED_DATAFRAME in parameters else None
    positive = parameters["positive"] if "positive" in parameters else True

    return filter_df_on_start_activities(df, values, timestamp_key=timestamp_key, case_id_glue=case_id_glue, activity_key=activity_key,
                                         positive=positive, grouped_df=grouped_df)


def apply_auto_filter(df, parameters=None):
    """Applies auto filter on end activities
    """

    if parameters is None:
        parameters = {}

    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    activity_key = parameters[
        PARAMETER_CONSTANT_ACTIVITY_KEY] if PARAMETER_CONSTANT_ACTIVITY_KEY in parameters else DEFAULT_NAME_KEY
    decreasing_factor = parameters[
        "decreasingFactor"] if "decreasingFactor" in parameters else DECREASING_FACTOR
    grouped_df = parameters[GROUPED_DATAFRAME] if GROUPED_DATAFRAME in parameters else None

    start_activities = get_start_activities(df, parameters=parameters)
    salist = start_activities_common.get_sorted_start_activities_list(start_activities)
    sathreshold = start_activities_common.get_start_activities_threshold(salist, decreasing_factor)

    return filter_df_on_start_activities_nocc(df, sathreshold, sa_count0=start_activities, timestamp_key=timestamp_key, case_id_glue=case_id_glue,
                                              activity_key=activity_key, grouped_df=grouped_df)


def get_start_activities(df, parameters=None):
    """Gets start activities count
    """

    if parameters is None:
        parameters = {}

    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    activity_key = parameters[
        PARAMETER_CONSTANT_ACTIVITY_KEY] if PARAMETER_CONSTANT_ACTIVITY_KEY in parameters else DEFAULT_NAME_KEY
    grouped_df = parameters[GROUPED_DATAFRAME] if GROUPED_DATAFRAME in parameters else df.groupby(case_id_glue)

    # Using with Window function (PROB: cannot handle with the given grouped_df)
    #w = Window().partitionBy(case_id_glue).orderBy(timestamp_key)
    #df_start = df.withColumn("rn", F.row_number().over(w))
    #df_start = df_start.filter(df_start["rn"] == 1).drop("rn")

    # Using join operation
    grouped_df = grouped_df.agg(F.min(timestamp_key).alias(timestamp_key))
    df_start = df.join(F.broadcast(grouped_df), grouped_df.columns)

    rdd_start = df_start.rdd.map(lambda row: row.asDict())
    rdd_start = rdd_start.map(lambda event: (event[activity_key], 1)).reduceByKey(lambda x, y : x + y)

    return rdd_start.collectAsMap()


def filter_df_on_start_activities(df, values, timestamp_key=DEFAULT_TIMESTAMP_KEY, case_id_glue=CASE_CONCEPT_NAME,
                                  activity_key=DEFAULT_NAME_KEY, grouped_df=None, positive=True):
    """Filters the Spark dataframe on start activities
    """

    if grouped_df is None:
        grouped_df = df.groupby(case_id_glue)

    # Using join operation
    grouped_df = grouped_df.agg(F.min(timestamp_key).alias(timestamp_key))
    df_start = df.join(F.broadcast(grouped_df), grouped_df.columns)
    df_start = df_start.filter(df_start[activity_key].isin(values))
    filtered_index = df_start.select(grouped_df.columns[0]).rdd.map(lambda x: x[0]).collect()

    # Using with Window function (PROB: cannot handle with the given grouped_df)
    #w = Window().partitionBy(case_id_glue).orderBy(timestamp_key)
    #df_start = df.withColumn("rn", F.row_number().over(w))
    #df_start = df_start.filter(df_start["rn"] == 1).drop("rn")
    #df_start = df_start.filter(df_start[activity_key].isin(values))
    #filtered_index = df_start.select(case_id_glue).rdd.map(lambda x: x[0]).collect()

    if positive:
        return df.filter(df[grouped_df.columns[0]].isin(filtered_index))
    return df.filter(~df[grouped_df.columns[0]].isin(filtered_index))


def filter_df_on_start_activities_nocc(df, nocc, sa_count0=None, timestamp_key=DEFAULT_TIMESTAMP_KEY,
                                       case_id_glue=CASE_CONCEPT_NAME, activity_key=DEFAULT_NAME_KEY, grouped_df=None):
    """Filters the Spark dataframe on start activities number of occurrences
    """

    if grouped_df is None:
        grouped_df = df.groupby(case_id_glue)
    if sa_count0 is None:
        parameters = {
            PARAMETER_CONSTANT_TIMESTAMP_KEY: timestamp_key,
            PARAMETER_CONSTANT_CASEID_KEY: case_id_glue,
            PARAMETER_CONSTANT_ACTIVITY_KEY: activity_key,
            GROUPED_DATAFRAME: grouped_df
        }
        sa_count0 = get_start_activities(df, parameters=parameters)
    sa_count = [k for k, v in sa_count0.items() if v >= nocc]

    # Using join operation
    grouped_df = grouped_df.agg(F.min(timestamp_key).alias(timestamp_key))
    df_start = df.join(F.broadcast(grouped_df), grouped_df.columns)
    if len(sa_count) < len(sa_count0):
        df_start = df_start.filter(df_start[activity_key].isin(sa_count))
        filtered_index = df_start.select(grouped_df.columns[0]).rdd.map(lambda x: x[0]).collect()
        return df.filter(df[grouped_df.columns[0]].isin(filtered_index))
    return df
