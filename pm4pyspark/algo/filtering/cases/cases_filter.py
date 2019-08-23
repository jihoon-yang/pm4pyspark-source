import pyspark.sql.functions as F




def filter_on_ncases(df, case_id_glue="case:concept:name", max_no_cases=1000):
    """Filters the Spark dataframe keeping only the specified maximum number of traces
    """

    # With conversion to RDD.
    #cases_to_keep = df.select(case_id_glue).distinct().rdd.map(lambda row : row[0]).collect()
    #cases_to_keep = cases_to_keep[0:min(len(cases_to_keep), max_no_cases)]
    #return df.filter(df[case_id_glue].isin(cases_to_keep))

    #Without conversion to RDD (better).
    grouped_df = df.groupBy(case_id_glue).count().limit(max_no_cases).drop("count")

    return df.join(F.broadcast(grouped_df), case_id_glue)


def filter_on_case_size(df, case_id_glue="case:concept:name", min_case_size=2, max_case_size=None):
    """Filters the Spark dataframe keeping only traces with at least the specified number of events
    """

    size_df = df.groupBy(case_id_glue).count()
    if max_case_size:
        size_df = size_df.filter((size_df["count"] >= min_case_size) & (size_df["count"] <= max_case_size))
    else:
        size_df = size_df.filter(size_df["count"] >= min_case_size)
    return df.join(F.broadcast(size_df), case_id_glue).drop("count")


def filter_on_case_performance(df, case_id_glue="case:concept:name", timestamp_key="time:timestamp",
                               min_case_performance=0, max_case_performance=10000000000):
    """Filters the Spark dataframe on case performance
    """

    grouped_df = df.groupby(case_id_glue)
    start_end_df = grouped_df.agg(F.min(timestamp_key).alias(timestamp_key), F.max(timestamp_key).alias(timestamp_key+"_1"))

    start_end_df = start_end_df.withColumn("caseDuration", F.unix_timestamp(start_end_df[timestamp_key+"_1"]) - F.unix_timestamp(start_end_df[timestamp_key]))
    start_end_df = start_end_df.filter((start_end_df["caseDuration"] > min_case_performance) & (start_end_df["caseDuration"] < max_case_performance))\
                               .select(case_id_glue)

    return df.join(F.broadcast(start_end_df), case_id_glue)


def apply(df, parameters=None):
    del df
    del parameters
    raise Exception("apply method not available for case filter")


def apply_auto_filter(df, parameters=None):
    del df
    del parameters
    raise Exception("apply_auto_filter method not available for case filter")
