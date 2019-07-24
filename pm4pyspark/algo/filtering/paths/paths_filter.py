import pyspark.sql.functions as F

from pm4py.algo.filtering.common.filtering_constants import CASE_CONCEPT_NAME
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_TIMESTAMP_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_ATTRIBUTE_KEY
from pm4py.util.constants import PARAMETER_CONSTANT_CASEID_KEY
from pyspark.sql.window import Window

def apply(df, paths, parameters=None):
    '''
    Apply a filter on traces containing / not containing a path
    '''

    if parameters is None:
        parameters = {}
    paths = [path[0] + "," + path[1] for path in paths]

    timestamp_key = parameters[
        PARAMETER_CONSTANT_TIMESTAMP_KEY] if PARAMETER_CONSTANT_TIMESTAMP_KEY in parameters else DEFAULT_TIMESTAMP_KEY
    case_id_glue = parameters[
        PARAMETER_CONSTANT_CASEID_KEY] if PARAMETER_CONSTANT_CASEID_KEY in parameters else CASE_CONCEPT_NAME
    attribute_key = parameters[
        PARAMETER_CONSTANT_ATTRIBUTE_KEY] if PARAMETER_CONSTANT_ATTRIBUTE_KEY in parameters else DEFAULT_NAME_KEY
    positive = parameters["positive"] if "positive" in parameters else True

    df_reduced = df.orderBy(case_id_glue, timestamp_key)
    df_reduced = df_reduced.select(case_id_glue, attribute_key)

    w = Window().partitionBy(df_reduced[case_id_glue]).orderBy(df_reduced[case_id_glue])
    df_reduced_shift = df_reduced.withColumn(case_id_glue + "_1", F.lag(case_id_glue, -1, 'NaN').over(w))
    df_reduced_shift = df_reduced_shift.withColumn(attribute_key + "_1", F.lag(attribute_key, -1, 'NaN').over(w))
    stacked_df = df_reduced_shift.withColumn("@@path", F.concat(df_reduced_shift[attribute_key], F.lit(","), df_reduced_shift[attribute_key + "_1"]))
    stacked_df = stacked_df.filter(stacked_df["@@path"].isin(paths))
    filtered_index = stacked_df.select(stacked_df[case_id_glue]).rdd.map(lambda x: x[0]).collect()

    if positive:
        return df.filter(df[case_id_glue].isin(filtered_index))
    else:
        return df.filter(~df[case_id_glue].isin(filtered_index))


def apply_auto_filter(df, parameters=None):
    del df
    del parameters
    raise Exception("apply_auto_filter method not available for paths filter on dataframe")
