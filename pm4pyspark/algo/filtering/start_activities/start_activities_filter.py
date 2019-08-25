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

    df_start = grouped_df.agg(F.first(activity_key).alias(activity_key)).select(activity_key)
    rdd_start = df_start.rdd.map(lambda row: (row[0], 1)).reduceByKey(lambda x, y : x + y)

    return rdd_start.collectAsMap()


def filter_df_on_start_activities(df, values, timestamp_key=DEFAULT_TIMESTAMP_KEY, case_id_glue=CASE_CONCEPT_NAME,
                                  activity_key=DEFAULT_NAME_KEY, grouped_df=None, positive=True):
    """Filters the Spark dataframe on start activities
    """

    if grouped_df is None:
        grouped_df = df.groupby(case_id_glue)

    grouped_df = grouped_df.agg(F.first(activity_key).alias(activity_key+"_1"))
    df_start = grouped_df.filter(grouped_df[activity_key+"_1"].isin(values))

    if positive:
        return df.join(F.broadcast(df_start), grouped_df.columns[0]).drop(activity_key+"_1")
    else:
        return df.join(F.broadcast(df_start), grouped_df.columns[0], "leftanti")


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

    if len(sa_count) < len(sa_count0):
        grouped_df = grouped_df.agg(F.first(activity_key).alias(activity_key+"_1"))
        df_start = grouped_df.filter(grouped_df[activity_key+"_1"].isin(sa_count))
        return df.join(F.broadcast(df_start), grouped_df.columns[0]).drop(activity_key+"_1")
    return df
