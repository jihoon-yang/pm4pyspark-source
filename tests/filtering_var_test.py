import os
import time
from tests.constants import INPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer

from pm4pyspark.algo.filtering.variants import variants_filter

spark_df = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "receipt.csv"), header=True)
spark_df.cache()

variants_df = variants_filter.get_variants_df(spark_df)
variants_df.show(variants_df.count())

print(variants_filter.get_variant_statistics(spark_df, parameters={'max_variants_to_return': 3}))
ddf, dlist = variants_filter.get_variants_df_and_list(spark_df)
print(dlist)

start_time = time.time()
variants_df2 = variants_filter.get_variants_df_with_case_duration(spark_df)
variants_df2.show()

event_with_caseid1 = variants_filter.get_events(spark_df, 1)
print(event_with_caseid1)
stat_with_duration = variants_filter.get_variant_statistics_with_case_duration(spark_df)
print(stat_with_duration)

case_description = variants_filter.get_cases_description(spark_df)
print(case_description)

applied_df = variants_filter.apply(spark_df, ["Confirmation of receipt,T02 Check confirmation of receipt,T04 Determine confirmation of receipt,T05 Print and send confirmation of receipt,T06 Determine necessity of stop advice,T10 Determine necessity to stop indication"])
variants_count_applied_df = variants_filter.get_variant_statistics(applied_df)
print(variants_count_applied_df)

auto_applied_df = variants_filter.apply_auto_filter(spark_df)
print(auto_applied_df.count())

spark_df.unpersist()
