import pyspark.sql.functions as F

from pm4py.algo.filtering.common.end_activities import end_activities_common
from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME, DECREASING_FACTOR
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_TIMESTAMP_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_ACTIVITY_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_CASEID_KEY
from pm4py.util.constants import PARAM_MOST_COMMON_VARIANT
from pm4py.util.constants import RETURN_EA_COUNT_DICT_AUTOFILTER
from pm4py.util.constants import GROUPED_DATAFRAME




def apply(df, values, parameters=None):
    """Filters the Spark dataframe on end activities
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

    return filter_df_on_end_activities(df, values, timestamp_key=timestamp_key, case_id_glue=case_id_glue, activity_key=activity_key,
                                       positive=positive, grouped_df=grouped_df)


def apply_auto_filter(df, parameters=None):
    """Applies auto filter on end activities
    """
    if parameters is None:
        parameters = {}

    most_common_variant = parameters[PARAM_MOST_COMMON_VARIANT] if PARAM_MOST_COMMON_VARIANT in parameters else None

    if most_common_variant is None:
        most_common_variant = []

    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    activity_key = parameters[
        PARAMETER_CONSTANT_ACTIVITY_KEY] if PARAMETER_CONSTANT_ACTIVITY_KEY in parameters else DEFAULT_NAME_KEY
    grouped_df = parameters[GROUPED_DATAFRAME] if GROUPED_DATAFRAME in parameters else None
    return_dict = parameters[
        RETURN_EA_COUNT_DICT_AUTOFILTER] if RETURN_EA_COUNT_DICT_AUTOFILTER in parameters else False

    decreasing_factor = parameters[
        "decreasingFactor"] if "decreasingFactor" in parameters else DECREASING_FACTOR
    if df.count() > 0:
        end_activities = get_end_activities(df, parameters=parameters)
        ealist = end_activities_common.get_sorted_end_activities_list(end_activities)
        eathreshold = end_activities_common.get_end_activities_threshold(ealist, decreasing_factor)

        return filter_df_on_end_activities_nocc(df, eathreshold, ea_count0=end_activities, timestamp_key=timestamp_key,
                                                case_id_glue=case_id_glue, activity_key=activity_key, grouped_df=grouped_df,
                                                return_dict=return_dict, most_common_variant=most_common_variant)

    if return_dict:
        return df, {}

    return df


def get_end_activities(df, parameters=None):
    """Gets end activities count
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

    df_end = grouped_df.agg(F.last(activity_key).alias(activity_key)).select(activity_key)
    rdd_end = df_end.rdd.map(lambda row: (row[0], 1)).reduceByKey(lambda x, y : x + y)

    return rdd_end.collectAsMap()


def filter_df_on_end_activities(df, values, timestamp_key=DEFAULT_TIMESTAMP_KEY, case_id_glue=CASE_CONCEPT_NAME,
                                activity_key=DEFAULT_NAME_KEY, grouped_df=None, positive=True):
    """Filters the Spark dataframe on end activities
    """

    if grouped_df is None:
        grouped_df = df.groupby(case_id_glue)

    grouped_df = grouped_df.agg(F.last(activity_key).alias(activity_key))
    df_end = grouped_df.filter(grouped_df[activity_key].isin(values))

    if positive:
        return df.join(F.broadcast(df_end), grouped_df.columns[0])
    else:
        return df.join(F.broadcast(df_end), grouped_df.columns[0], "leftanti")


def filter_df_on_end_activities_nocc(df, nocc, ea_count0=None, timestamp_key=DEFAULT_TIMESTAMP_KEY,
                                     case_id_glue=CASE_CONCEPT_NAME, activity_key=DEFAULT_NAME_KEY,
                                     grouped_df=None, return_dict=False, most_common_variant=None):
    """Filters the Spark dataframe on end activities number of occurrences
    """

    if most_common_variant is None:
        most_common_variant = []

    if df.count() > 0:
        if grouped_df is None:
            grouped_df = df.groupby(case_id_glue)
        if ea_count0 is None:
            parameters = {
                PARAMETER_CONSTANT_TIMESTAMP_KEY: timestamp_key,
                PARAMETER_CONSTANT_CASEID_KEY: case_id_glue,
                PARAMETER_CONSTANT_ACTIVITY_KEY: activity_key,
                GROUPED_DATAFRAME: grouped_df
            }
            ea_count0 = get_end_activities(df, parameters=parameters)
        ea_count = [k for k, v in ea_count0.items() if
                    v >= nocc or (len(most_common_variant) > 0 and k == most_common_variant[-1])]
        ea_count_dict = {k: v for k, v in ea_count0.items() if
                         v >= nocc or (len(most_common_variant) > 0 and k == most_common_variant[-1])}

        # Using join operation
        if len(ea_count) < len(ea_count0):
            grouped_df = grouped_df.agg(F.last(activity_key).alias(activity_key))
            df_end = grouped_df.filter(grouped_df[activity_key].isin(ea_count))
            if return_dict:
                return df.join(F.broadcast(df_end), grouped_df.columns[0]), ea_count_dict
            return df.join(F.broadcast(df_end), grouped_df.columns[0])
        if return_dict:
            return df, ea_count_dict
    return df
