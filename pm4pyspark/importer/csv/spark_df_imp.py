from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def import_sparkdf_from_path(path, sep=None, quote=None, header=None, inferSchema=None):
    """
    Imports a Spark Dataframe from the given path of csv format file
    (with selecting the most important columns:
        "case:concept:name"
        "concept:name"
        "time:timestamp"
    and order by timestamp)

    Parameters
    ----------
    path:
        Input CSV file path
    sep:
        column separator
    quote:
        sets a single character used for escaping quoted values where the
        separator can be part of the value. If None is set, it uses the default
        value, ``"``. If an empty string is set, it uses ``u0000`` (null character).
    header:
        uses the first line as names of columns. If None is set, it uses the
        default value, ``False``.
    inferSchema:
        infers the input schema automatically from data. It requires one extra
        pass over the data. If None is set, it uses the default value, ``False``.

    Returns
    -------
    spark_df
        Spark Dataframe
    """
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.read.csv(path, sep=sep, quote=quote, header=header, inferSchema=inferSchema)
    spark_df = spark_df.select("case:concept:name", "concept:name", F.to_timestamp(spark_df['time:timestamp']).alias('time:timestamp'))\
        .orderBy("time:timestamp")

    return spark_df

def import_log(path, parameters=None):
    """
    Imports a log from the given path of csv format file

    Parameters
    ----------
    path:
        Input CSV file path
    parameters:
        sep     ->  column separator in CSV
        quote   ->  sets a single character used for escaping quoted values where the
                    separator can be part of the value in CSV. If None is set, it uses the default
                    value, ``"``. If an empty string is set, it uses ``u0000`` (null character).
        header  ->  uses the first line as names of columns in CSV. If None is set, it uses the
                    default value, ``False``.
        inferSchema ->  infers the input schema automatically from data in CSV. It requires one extra
                        pass over the data. If None is set, it uses the default value, ``False``.

    Returns
    -------
    pair_rdd_group.collect()
        An event log as a list
    """

    sep = None
    quote = None
    header = False

    if parameters is None:
        parameters = {}
    if "sep" in parameters:
        sep = parameters["sep"]
    if "quote" in parameters:
        quote = parameters["quote"]
    if "header" in parameters:
        header = parameters["header"]
    if "inferSchema" in parameters:
        inferSchema = parameters["inferSchema"]

    spark_df = import_sparkdf_from_path(path, sep=sep, quote=quote, header=header, inferSchema=inferSchema)
    rdd = spark_df.rdd.map(list)
    pair_rdd = rdd.map(lambda s: (s[0], (s[1], s[2])))
    pair_rdd_group = pair_rdd.groupByKey().mapValues(list)

    return pair_rdd_group.collect()
