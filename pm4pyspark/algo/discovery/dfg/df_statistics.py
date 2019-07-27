import pyspark.sql.functions as F

from pyspark.sql.window import Window




def get_dfg_graph(df, measure="frequency", activity_key="concept:name", case_id_glue="case:concept:name",
                  timestamp_key="time:timestamp", perf_aggregation_key="mean", sort_caseid_required=True,
                  sort_timestamp_along_case_id=True, window=1):
    """Gets DFG graph from the Spark dataframe
    """

    if sort_caseid_required:
        if sort_timestamp_along_case_id:
            df = df.orderBy(case_id_glue, timestamp_key)
        else:
            df = df.orderBy(case_id_glue)

    if measure == "frequency":
        df_reduced = df.select(case_id_glue, activity_key)
    else:
        df_reduced = df.select(case_id_glue, activity_key, timestamp_key)

    w = Window().partitionBy(df_reduced[case_id_glue]).orderBy(df_reduced[case_id_glue])
    df_reduced_shift = df_reduced.withColumn(case_id_glue + "_1", F.lag(case_id_glue, -window, 'NaN').over(w))
    df_reduced_shift = df_reduced_shift.withColumn(activity_key + "_1", F.lag(activity_key, -window, 'NaN').over(w))
    if measure != "frequency":
        df_reduced_shift = df_reduced_shift.withColumn(timestamp_key + "_1", F.lag(timestamp_key, -window, 'NaN').over(w))
    df_successive_rows = df_reduced_shift.filter(df_reduced_shift[case_id_glue] == df_reduced_shift[case_id_glue + "_1"])

    if measure == "performance" or measure == "both":
        df_successive_rows = df_successive_rows.withColumn("caseDuration",
                                                           F.unix_timestamp(df_successive_rows[timestamp_key+"_1"])
                                                           - F.unix_timestamp(df_successive_rows[timestamp_key]))
    directly_follows_grouping = df_successive_rows.groupby(activity_key, activity_key + "_1")


    dfg_frequency = {}
    dfg_performance = {}

    if measure == "frequency" or measure == "both":
        dfg_frequency = directly_follows_grouping.count().rdd.map(lambda row: ((row[0], row[1]), row[2]))

    if measure == "performance" or measure == "both":
        dfg_performance = directly_follows_grouping.agg({"caseDuration":perf_aggregation_key})
        dfg_performance = dfg_performance.rdd.map(lambda row: ((row[0], row[1]), row[2]))

    if measure == "frequency":
        return dfg_frequency.collectAsMap()

    if measure == "performance":
        return dfg_performance.collectAsMap()

    if measure == "both":
        return [dfg_frequency.collectAsMap(), dfg_performance.collectAsMap()]
