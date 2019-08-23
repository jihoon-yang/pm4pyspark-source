import pyspark.sql.functions as F

from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME, DECREASING_FACTOR
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY
from pm4py.statistics.traces.common import case_duration as case_duration_commons
from pm4py.util.constants import PARAMETER_CONSTANT_TIMESTAMP_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_ACTIVITY_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_CASEID_KEY
from pyspark.sql.window import Window




def apply_auto_filter(df, parameters=None):
    """Applies an automatic filter on variants
    """
    if parameters is None:
        parameters = {}
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    variants_df = get_variants_df(df, parameters=parameters)
    parameters["variants_df"] = variants_df
    variants = get_variant_statistics(df, parameters=parameters)
    decreasing_factor = parameters[
        "decreasingFactor"] if "decreasingFactor" in parameters else DECREASING_FACTOR

    admitted_variants = []
    if len(variants) > 0:
        current_variant_count = variants[0][case_id_glue]

        for i in range(len(variants)):
            if variants[i][case_id_glue] >= decreasing_factor * current_variant_count:
                admitted_variants.append(variants[i]["variant"])
            else:
                break
            current_variant_count = variants[i][case_id_glue]

    return apply(df, admitted_variants, parameters=parameters)


def apply(df, admitted_variants, parameters=None):
    """Applies a filter on variants
    """
    if parameters is None:
        parameters = {}

    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    positive = parameters["positive"] if "positive" in parameters else True
    variants_df = parameters["variants_df"] if "variants_df" in parameters else get_variants_df(df,
                                                                                                parameters=parameters)
    variants_df = variants_df.filter(variants_df["variant"].isin(admitted_variants))

    if positive:
        return df.join(F.broadcast(variants_df), case_id_glue)
    else:
        return df.join(F.broadcast(variants_df), case_id_glue, "leftanti")


def get_variant_statistics(df, parameters=None):
    """Gets variants from the Spark dataframe
    """
    if parameters is None:
        parameters = {}

    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    max_variants_to_return = parameters["max_variants_to_return"] if "max_variants_to_return" in parameters else None
    variants_df = parameters["variants_df"] if "variants_df" in parameters else get_variants_df(df,
                                                                                                parameters=parameters)

    variants_df_count = variants_df.groupby("variant").count().orderBy("count", ascending=False)
    variants_df_count = variants_df_count.withColumnRenamed("count", case_id_glue)
    rdd = variants_df_count.rdd.map(lambda row: row.asDict())
    if max_variants_to_return:
        return rdd.take(max_variants_to_return)
    return rdd.collect()


def get_variant_statistics_with_case_duration(df, parameters=None):
    """Gets variants from the Spark dataframe with case duration
    """
    if parameters is None:
        parameters = {}
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    max_variants_to_return = parameters["max_variants_to_return"] if "max_variants_to_return" in parameters else None
    variants_df = parameters["variants_df"] if "variants_df" in parameters else get_variants_df_with_case_duration(df,
                                                                                                                   parameters=parameters)

    variants_df = variants_df.groupby("variant").agg(
                                                     F.mean("caseDuration").alias("caseDuration"),
                                                     F.count(F.lit(1)).alias("count")
                                                 ).orderBy("count", ascending=False)
    variants_list = variants_df.rdd.map(lambda row: row.asDict())
    if max_variants_to_return:
        return variants_list.take(max_variants_to_return)
    return variants_list.collect()


def get_variants_df_and_list(df, parameters=None):
    """(Technical method) Provides variants_df and variants_list out of the box
    """
    if parameters is None:
        parameters = {}
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    variants_df = get_variants_df(df, parameters=parameters)
    parameters["variants_df"] = variants_df
    variants_stats = get_variant_statistics(df, parameters=parameters)
    variants_list = []
    for vd in variants_stats:
        variant = vd["variant"]
        count = vd[case_id_glue]
        variants_list.append([variant, count])
        variants_list = sorted(variants_list, key=lambda x: (x[1], x[0]), reverse=True)
    return variants_df, variants_list


def get_cases_description(df, parameters=None):
    """Gets a description of traces present in the Spark dataframe
    """
    if parameters is None:
        parameters = {}

    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    enable_sort = parameters["enable_sort"] if "enable_sort" in parameters else True
    sort_by_column = parameters["sort_by_column"] if "sort_by_column" in parameters else "startTime"
    sort_ascending = parameters["sort_ascending"] if "sort_ascending" in parameters else True
    max_ret_cases = parameters["max_ret_cases"] if "max_ret_cases" in parameters else None

    ordered_df = df.orderBy(timestamp_key).select(case_id_glue, timestamp_key)
    grouped_df = ordered_df.groupby(case_id_glue)

    start_df = grouped_df.agg(F.min(timestamp_key).alias(timestamp_key))
    first_eve_df = ordered_df.join(F.broadcast(start_df), start_df.columns)
    end_df = grouped_df.agg(F.max(timestamp_key).alias(timestamp_key))
    last_eve_df = ordered_df.join(F.broadcast(end_df), end_df.columns)
    last_eve_df = last_eve_df.withColumnRenamed(timestamp_key, timestamp_key+"_2")

    stacked_df = first_eve_df.join(last_eve_df, case_id_glue).orderBy(case_id_glue)
    stacked_df = stacked_df.withColumn("caseDuration", F.unix_timestamp(stacked_df[timestamp_key+"_2"]) - F.unix_timestamp(stacked_df[timestamp_key]))
    stacked_df = stacked_df.withColumn("startTime", F.unix_timestamp(stacked_df[timestamp_key])).drop(timestamp_key)
    stacked_df = stacked_df.withColumn("endTime", F.unix_timestamp(stacked_df[timestamp_key+"_2"])).drop(timestamp_key+"_2")

    if enable_sort:
        stacked_df = stacked_df.orderBy(sort_by_column, ascending=sort_ascending)
    if max_ret_cases is not None:
        stacked_df = stacked_df.limit(max_ret_cases)
    rdd = stacked_df.rdd.map(lambda x: (x[case_id_glue], {'caseDuration': x['caseDuration'],
                                               'startTime': x['startTime'],
                                               'endTime': x['endTime']}))
    return rdd.collectAsMap()


def get_variants_df(df, parameters=None):
    """Gets variants dataframe from the Spark dataframe
    """
    if parameters is None:
        parameters = {}

    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    activity_key = parameters[
        PARAMETER_CONSTANT_ACTIVITY_KEY] if PARAMETER_CONSTANT_ACTIVITY_KEY in parameters else DEFAULT_NAME_KEY

    df = df.select(case_id_glue, activity_key)
    grouped_df = df.withColumn("@@id", F.monotonically_increasing_id())\
        .groupBy(case_id_glue)\
        .agg(F.collect_list(F.struct("@@id", activity_key)).alias("variant"))\
        .select(case_id_glue, F.sort_array("variant").getItem(activity_key).alias("variant"))
    grouped_df = grouped_df.withColumn("variant", F.concat_ws(",", "variant"))

    return grouped_df


def get_variants_df_with_case_duration(df, parameters=None):
    """Gets variants dataframe from the Spark dataframe, with case duration that is included
    """
    if parameters is None:
        parameters = {}

    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    activity_key = parameters[
        PARAMETER_CONSTANT_ACTIVITY_KEY] if PARAMETER_CONSTANT_ACTIVITY_KEY in parameters else DEFAULT_NAME_KEY
    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY

    ordered_df = df.orderBy(timestamp_key).select(case_id_glue, timestamp_key, activity_key)
    grouped_df = ordered_df.groupby(case_id_glue)
    df1 = grouped_df.agg(F.collect_list(activity_key).alias("variant"))
    df1 = df1.withColumn("variant", F.concat_ws(",", "variant")).orderBy(case_id_glue)

    start_df = grouped_df.agg(F.min(timestamp_key).alias(timestamp_key))
    first_eve_df = ordered_df.join(F.broadcast(start_df), start_df.columns)
    end_df = grouped_df.agg(F.max(timestamp_key).alias(timestamp_key))
    last_eve_df = ordered_df.join(F.broadcast(end_df), end_df.columns)
    last_eve_df = last_eve_df.withColumnRenamed(timestamp_key, timestamp_key+"_2")
    last_eve_df = last_eve_df.withColumnRenamed(activity_key, activity_key+"_2")

    stacked_df = first_eve_df.join(last_eve_df, case_id_glue).orderBy(case_id_glue)
    stacked_df = stacked_df.withColumn("caseDuration", F.unix_timestamp(stacked_df[timestamp_key+"_2"]) - F.unix_timestamp(stacked_df[timestamp_key]))
    new_df = df1.join(stacked_df, case_id_glue)
    return new_df


def get_events(df, case_id, parameters=None):
    """Gets events belonging to the specified case
    """
    if parameters is None:
        parameters = {}
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    return df.filter(df[case_id_glue] == case_id).rdd.map(lambda row: row.asDict()).collect()


def get_kde_caseduration(df, parameters=None):
    """Gets the estimation of KDE density for the case durations calculated on the Spark dataframe
    """
    cases = get_cases_description(df, parameters=parameters)
    duration_values = [x["caseDuration"] for x in cases.values()]

    return case_duration_commons.get_kde_caseduration(duration_values, parameters=parameters)


def get_kde_caseduration_json(df, parameters=None):
    """
    Gets the estimation of KDE density for the case durations calculated on the Spark dataframe
    (expressed as JSON)
    """
    cases = get_cases_description(df, parameters=parameters)
    duration_values = [x["caseDuration"] for x in cases.values()]

    return case_duration_commons.get_kde_caseduration_json(duration_values, parameters=parameters)
