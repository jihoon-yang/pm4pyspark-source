import pyspark.sql.functions as F

from pm4py.visualization.dfg import factory as dfg_vis_factory
from pyspark.sql.window import Window


def get_freq_tuples(df):
    '''
    Gets frequency tuples from a Spark DataFrame
    '''

    df_reduced = df.orderBy("case:concept:name", "time:timestamp")
    df_reduced = df_reduced.select("case:concept:name", "concept:name")
    w = Window().partitionBy(df_reduced["case:concept:name"]).orderBy(df_reduced["case:concept:name"])
    df_reduced_shift = df_reduced.withColumn("case:concept:name_1", F.lag("case:concept:name", -1, 'NaN').over(w))
    df_reduced_shift = df_reduced_shift.withColumn("concept:name_1", F.lag("concept:name", -1, 'NaN').over(w))
    df_successive_rows = df_reduced_shift.filter(df_reduced_shift["case:concept:name"] == df_reduced_shift["case:concept:name_1"])
    directly_follows_grouping = df_successive_rows.groupby("concept:name", "concept:name_1")
    directly_follows_grouping = directly_follows_grouping.count()

    rdd_directly_follows_grouping = directly_follows_grouping.rdd.map(list)
    rdd_directly_follows_grouping = rdd_directly_follows_grouping.map(lambda row: ((str(row[0]), str(row[1])), int(row[2])))
    freq_tuples = rdd_directly_follows_grouping.collectAsMap()

    return freq_tuples

def show_dfg(df, log=None, variant="frequency"):
    '''
    Returns a DFG of the given Spark DataFrame and its EventLog (optional).
    '''

    freq_tuples = get_freq_tuples(df)
    gviz = dfg_vis_factory.apply(freq_tuples, log=log, variant=variant)
    dfg_vis_factory.view(gviz)

def save_dfg(df, path, log=None, variant="frequency", parameters=None):
    '''
    Saves a DFG of the given Spark DataFrame and its EventLog (optional) at the given path.
    '''

    if parameters is None:
        parameters = {"format":"svg"}
    else:
        parameters = parameters
    freq_tuples = get_freq_tuples(df)
    gviz = dfg_vis_factory.apply(freq_tuples, log=log, variant=variant, parameters=parameters)
    dfg_vis_factory.save(gviz, path)
