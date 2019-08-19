import pyspark.sql.functions as F

from pm4py.algo.filtering.common.attributes import attributes_common
from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME, DECREASING_FACTOR
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_ATTRIBUTE_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_CASEID_KEY
from pm4py.util.constants import PARAM_MOST_COMMON_VARIANT




def apply_numeric_events(df, int1, int2, parameters=None):
    """Applies a filter on events (numerical filter)
    """

    if parameters is None:
        parameters = {}
    attribute_key = parameters[
        PARAMETER_CONSTANT_ATTRIBUTE_KEY] if PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters else DEFAULT_NAME_KEY
    positive = parameters["positive"] if "positive" in parameters else True
    if positive:
        return df.filter(df[attribute_key].between(int1, int2))
    else:
        return df.filter(~df[attribute_key].between(int1, int2))


def apply_numeric(df, int1, int2, parameters=None):
    """Filters the Spark dataframe on attribute values (filter cases)
    """

    if parameters is None:
        parameters = {}
    attribute_key = parameters[
        PARAMETER_CONSTANT_ATTRIBUTE_KEY] if PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters else DEFAULT_NAME_KEY
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    positive = parameters["positive"] if "positive" in parameters else True

    df_filtered = df.filter(df[attribute_key].between(int1, int2))
    df_filtered = df_filtered.groupBy(case_id_glue).count()
    #filtered_index = df_filtered.select(case_id_glue).rdd.map(lambda x: x[0]).collect()
    if positive:
        return df.join(F.broadcast(df_filtered), case_id_glue).drop("count")
    else:
        df_left_joined = df.join(F.broadcast(df_filtered), case_id_glue, "left")
        return df_left_joined.filter(df_left_joined["count"].isNull()).drop("count")


def apply_events(df, values, parameters=None):
    """Filters the Spark dataframe on attribute values (filter events)
    """

    if parameters is None:
        parameters = {}
    attribute_key = parameters[
        PARAMETER_CONSTANT_ATTRIBUTE_KEY] if PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters else DEFAULT_NAME_KEY
    positive = parameters["positive"] if "positive" in parameters else True
    if positive:
        return df.filter(df[attribute_key].isin(values))
    else:
        return df.filter(~df[attribute_key].isin(values))


def apply(df, values, parameters=None):
    """Filters the Spark dataframe on attribute values (filter traces)
    """

    if parameters is None:
        parameters = {}

    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    attribute_key = parameters[
        PARAMETER_CONSTANT_ATTRIBUTE_KEY] if PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters else DEFAULT_NAME_KEY
    positive = parameters["positive"] if "positive" in parameters else True

    return filter_df_on_attribute_values(df, values, case_id_glue=case_id_glue, attribute_key=attribute_key,
                                         positive=positive)


def apply_auto_filter(df, parameters=None):
    """Applies auto filter on activity values
    """
    if parameters is None:
        parameters = {}

    most_common_variant = parameters[PARAM_MOST_COMMON_VARIANT] if PARAM_MOST_COMMON_VARIANT in parameters else None

    if most_common_variant is None:
        most_common_variant = []

    activity_key = parameters[
        PARAMETER_CONSTANT_ACTIVITY_KEY] if PARAMETER_CONSTANT_ACTIVITY_KEY in parameters else DEFAULT_NAME_KEY
    decreasing_factor = parameters[
        "decreasingFactor"] if "decreasingFactor" in parameters else DECREASING_FACTOR

    if df.count() > 0:
        activities = get_attribute_values(df, activity_key)
        alist = attributes_common.get_sorted_attributes_list(activities)
        thresh = attributes_common.get_attributes_threshold(alist, decreasing_factor)

        return filter_df_keeping_activ_exc_thresh(df, thresh, activity_key=activity_key, act_count0=activities,
                                                  most_common_variant=most_common_variant)
    return df


def get_attribute_values(df, attribute_key, parameters=None):
    """Returns a list of attribute values contained in the specified column of the CSV
    """

    if parameters is None:
        parameters = {}
    str(parameters)
    df = df.select(attribute_key)
    rdd_df = df.rdd.map(lambda event: (event[0], 1)).reduceByKey(lambda x, y : x + y)\
                                                    .sortBy(lambda x: -x[1])

    return rdd_df.collectAsMap()


def filter_df_on_attribute_values(df, values, case_id_glue="case:concept:name", attribute_key="concept:name",
                                  positive=True):
    """Filters the Spark dataframe on attribute values
    """

    df_filtered = df.filter(df[attribute_key].isin(values))
    df_filtered = df_filtered.groupBy(case_id_glue).count()
    if positive:
        return df.join(F.broadcast(df_filtered), case_id_glue).drop("count")
    else:
        df_left_joined = df.join(F.broadcast(df_filtered), case_id_glue, "left")
        return df_left_joined.filter(df_left_joined["count"].isNull()).drop("count")


def filter_df_keeping_activ_exc_thresh(df, thresh, act_count0=None, activity_key="concept:name",
                                       most_common_variant=None):
    """Filters the Spark dataframe keeping activities exceeding the threshold
    """

    if most_common_variant is None:
        most_common_variant = []

    if act_count0 is None:
        act_count0 = get_attribute_values(df, activity_key)
    act_count = [k for k, v in act_count0.items() if v >= thresh or k in most_common_variant]
    if len(act_count) < len(act_count0):
        df = df.filter(df[activity_key].isin(act_count))
    return df


def filter_df_keeping_spno_activities(df, activity_key="concept:name", max_no_activities=25):
    """Filters the Spark dataframe on the specified number of attributes
    """

    activity_values_dict = get_attribute_values(df, activity_key)
    activity_values_ordered_list = []
    for act in activity_values_dict:
        activity_values_ordered_list.append([act, activity_values_dict[act]])
    activity_values_ordered_list = sorted(activity_values_ordered_list, key=lambda x: (x[1], x[0]), reverse=True)
    activity_values_ordered_list = activity_values_ordered_list[
                                   0:min(len(activity_values_ordered_list), max_no_activities)]
    activity_to_keep = [x[0] for x in activity_values_ordered_list]

    if len(activity_to_keep) < len(activity_values_dict):
        df = df.filter(df[activity_key].isin(activity_to_keep))
    return df


def get_kde_numeric_attribute(df, attribute, parameters=None):
    """Gets the KDE estimation for the distribution of a numeric attribute values
    """
    values = df.select(attribute).rdd.map(lambda row : row[0]).collect()

    return attributes_common.get_kde_numeric_attribute(values, parameters=parameters)


def get_kde_numeric_attribute_json(df, attribute, parameters=None):
    """
    Gets the KDE estimation for the distribution of a numeric attribute values
    (expressed as JSON)
    """
    values = df.select(attribute).rdd.map(lambda row : row[0]).collect()

    return attributes_common.get_kde_numeric_attribute_json(values, parameters=parameters)


def get_kde_date_attribute(df, attribute=DEFAULT_TIMESTAMP_KEY, parameters=None):
    """Gets the KDE estimation for the distribution of a date attribute values
    """
    date_values = df.select(attribute).rdd.map(lambda row : row[0]).collect()

    return attributes_common.get_kde_date_attribute(date_values, parameters=parameters)


def get_kde_date_attribute_json(df, attribute=DEFAULT_TIMESTAMP_KEY, parameters=None):
    """
    Gets the KDE estimation for the distribution of a date attribute values
    (expressed as JSON)
    """
    values = df.select(attribute).rdd.map(lambda row : row[0]).collect()

    return attributes_common.get_kde_date_attribute_json(values, parameters=parameters)
