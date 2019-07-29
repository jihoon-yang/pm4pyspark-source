import os
from tests.constants import INPUT_DATA_DIR, OUTPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer

from pm4pyspark.algo.filtering.paths import paths_filter


df_ex = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True, inferSchema=True)
filtered_df = paths_filter.apply(df_ex, [('check ticket', 'decide')])
filtered_df.show(filtered_df.count(), truncate=False)
