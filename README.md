# Big Data Process Mining in Python <br/> <span style="font-size:0.7em">Integration of Spark in PM4PY for Preprocessing Event Data and Discover Process Models </span>


[PM4Py](https://github.com/pm4py) is the Process Mining library in Python and it aims at seamless integration with any kind of databases and technology.

**PM4PySpark** is the integration of [*Apache Spark*](https://spark.apache.org) in PM4Py. Especially, this Big Data connectors for PM4Py has a focus on embracing the big data world and to handle huge amount of data, with a particular focus on the Spark ecosystem:

- Loading CSV files into Apache Spark
- Loading and writing Parquet files into Apache Spark
- Calculating in an efficient way the Directly Follows Graph (DFG) on top of Apache Spark DataFrames
- Managing filtering operations (timeframe, attributes, start/end activities, paths, variants, cases) on top of Apache Spark
