import os
import time
import pandas as pd
from tests.constants import INPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer
from datetime import datetime

from pm4pyspark.algo.filtering.timestamp import timestamp_filter

spark_df = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "receipt.csv"), header=True)
spark_df.cache()

df_timest_contained = timestamp_filter.filter_traces_contained(spark_df, "2011-03-09 00:00:00", "2012-01-18 23:59:59")
print(df_timest_contained.count())
print(df_timest_contained.groupby("case:concept:name").count().count())

df_timest_intersecting = timestamp_filter.filter_traces_intersecting(spark_df, "2011-03-09 00:00:00", "2012-01-18 23:59:59")
df_timest_intersecting.show()

spark_df.unpersist()
