import os
from tests.constants import INPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer

from pm4pyspark.algo.filtering.start_activities import start_activities_filter


spark_df = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "receipt.csv"), header=True, inferSchema=True)
spark_df.cache()
print(start_activities_filter.get_start_activities(spark_df, parameters={"grouped_dataframe": spark_df.groupby('org:resource')}))
print(start_activities_filter.get_start_activities(spark_df))
filtered_df = start_activities_filter.filter_df_on_start_activities(spark_df, ["check ticket", "decide", "register request"])
filtered_df_nocc = start_activities_filter.filter_df_on_start_activities_nocc(spark_df, 6)
applied_auto_df = start_activities_filter.apply_auto_filter(spark_df)
spark_df.unpersist()
