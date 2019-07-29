import pyspark.sql.functions as F




def filter_on_ncases(df, case_id_glue="case:concept:name", max_no_cases=1000):
    """Filters the Spark dataframe keeping only the specified maximum number of traces
    """

    # With conversion to RDD.
    #cases_to_keep = df.select(case_id_glue).distinct().rdd.map(lambda row : row[0]).collect()
    #cases_to_keep = cases_to_keep[0:min(len(cases_to_keep), max_no_cases)]
    #return df.filter(df[case_id_glue].isin(cases_to_keep))

    #Without conversion to RDD (better).
    grouped_df = df.groupBy(case_id_glue).count().limit(max_no_cases)
    return df.join(F.broadcast(grouped_df), case_id_glue).drop("count")


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

    ordered_df = df.orderBy(timestamp_key).select(case_id_glue, timestamp_key)
    grouped_df = ordered_df.groupby(case_id_glue)

    start_df = grouped_df.agg(F.min(timestamp_key).alias(timestamp_key))
    first_eve_df = ordered_df.join(F.broadcast(start_df), start_df.columns)
    end_df = grouped_df.agg(F.max(timestamp_key).alias(timestamp_key))
    last_eve_df = ordered_df.join(F.broadcast(end_df), end_df.columns)
    last_eve_df = last_eve_df.withColumnRenamed(timestamp_key, timestamp_key+"_2")

    stacked_df = first_eve_df.join(last_eve_df, case_id_glue)
    stacked_df = stacked_df.withColumn("caseDuration", F.unix_timestamp(stacked_df[timestamp_key+"_2"]) - F.unix_timestamp(stacked_df[timestamp_key]))
    stacked_df = stacked_df.filter((stacked_df["caseDuration"] > min_case_performance) & (stacked_df["caseDuration"] < max_case_performance))
    stacked_df = stacked_df.select(case_id_glue)

    return df.join(F.broadcast(stacked_df), case_id_glue)


def apply(df, parameters=None):
    del df
    del parameters
    raise Exception("apply method not available for case filter")


def apply_auto_filter(df, parameters=None):
    del df
    del parameters
    raise Exception("apply_auto_filter method not available for case filter")
