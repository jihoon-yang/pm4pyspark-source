import os
from tests.constants import INPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer

from pm4pyspark.algo.filtering.end_activities import end_activities_filter


spark_df = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True, inferSchema=True)
spark_df.cache()


end_ac = end_activities_filter.get_end_activities(spark_df)
filtered_df = end_activities_filter.filter_df_on_end_activities(spark_df, {'T07-5 Draft intern advice aspect 5'})
filtered_df_apply = end_activities_filter.apply(spark_df, ["T05 Print and send confirmation of receipt", "T10 Determine necessity to stop indication"])
print(filtered_df_apply.count())
print(filtered_df_apply.groupby("case:concept:name").count().count())

filtered_df_nocc, rdict = end_activities_filter.filter_df_on_end_activities_nocc(spark_df, 400, return_dict=True)
filtered_df_nocc.show(filtered_df_nocc.count())
print(filtered_df_nocc.count())
print(rdict)

filtered_auto_filter = end_activities_filter.apply_auto_filter(spark_df)
print(filtered_auto_filter.count())
spark_df.unpersist()
