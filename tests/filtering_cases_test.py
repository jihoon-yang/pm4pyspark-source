import os
from tests.constants import INPUT_DATA_DIR, OUTPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer

from pm4pyspark.algo.filtering.cases import cases_filter


spark_df = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True, inferSchema=True)
spark_df.cache()

cases_filter.filter_on_ncases(spark_df, max_no_cases=3).show()

case_size_df = cases_filter.filter_on_case_size(spark_df, min_case_size=9, max_case_size=9)

perf_df = cases_filter.filter_on_case_performance(spark_df, max_case_performance=800000)

spark_df.unpersist()
