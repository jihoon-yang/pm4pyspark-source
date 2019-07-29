import os
import time
from tests.constants import INPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer

from pm4pyspark.algo.filtering.attributes import attributes_filter

spark_df = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True, inferSchema=True)
spark_df.cache()

activities = attributes_filter.get_attribute_values(spark_df, attribute_key="concept:name")
resources = attributes_filter.get_attribute_values(spark_df, attribute_key="org:resource")

filtered_df = attributes_filter.filter_df_on_attribute_values(spark_df, {'examine casually'}, positive=False)
filtered_df.show(filtered_df.count())

filtered_num_df = attributes_filter.apply_numeric_events(spark_df, 50, 100, parameters={"pm4py:param:attribute_key":"Costs"})

filtered_num_tr_df2 = attributes_filter.apply_numeric(spark_df, 0, 7000, parameters={"pm4py:param:attribute_key":"_c0"})

filtered_event_df = attributes_filter.apply_events(spark_df, values={"examine casually"})

filtered_thresh_df = attributes_filter.filter_df_keeping_activ_exc_thresh(spark_df, 7, most_common_variant={"reject request"})

filtered_top_5_act_df = attributes_filter.filter_df_keeping_spno_activities(spark_df, max_no_activities=5)

print(attributes_filter.get_kde_date_attribute(spark_df))

spark_df.unpersist()
