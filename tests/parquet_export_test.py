import os
import time

from tests.constants import INPUT_DATA_DIR, OUTPUT_DATA_DIR
from pm4pyspark.importer.parquet import spark_df_imp as parquet_importer
from pm4pyspark.exporter.parquet import spark_df_exp as parquet_exporter




dir_path = os.path.join(INPUT_DATA_DIR, "receipt")
spark_df_dir = parquet_importer.import_sparkdf_from_path(dir_path)
print(spark_df_dir.count())

out_path = os.path.join(OUTPUT_DATA_DIR, "receipt128")
parquet_exporter.export_sparkdf(spark_df_dir, out_path, mode="overwrite")

out_path2 = os.path.join(OUTPUT_DATA_DIR, "receipt64")
parquet_exporter.export_sparkdf(spark_df_dir, out_path2, num_partitions=64, mode="overwrite")


test_df_128 = parquet_importer.import_sparkdf_from_path(out_path)
test_df_64 = parquet_importer.import_sparkdf_from_path(out_path2)
print(test_df_128.count())
print(test_df_64.count())
