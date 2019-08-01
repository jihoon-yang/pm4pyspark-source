import os
import time

from tests.constants import INPUT_DATA_DIR
from pm4pyspark.importer.parquet import spark_df_imp as parquet_importer




file_path = os.path.join(INPUT_DATA_DIR, "running-example.parquet")
file_path2 = os.path.join(INPUT_DATA_DIR, "receipt.parquet")
dir_path = os.path.join(INPUT_DATA_DIR, "receipt")


spark_df = parquet_importer.import_sparkdf_from_path(file_path)
spark_df_sorted = parquet_importer.import_sparkdf_from_path(file_path, sort=True)

spark_df1 = parquet_importer.import_sparkdf_from_path(file_path2)
spark_df_sorted1 = parquet_importer.import_sparkdf_from_path(file_path2, sort=True)

spark_df_dir = parquet_importer.import_sparkdf_from_path(dir_path)
spark_df_dir_sorted = parquet_importer.import_sparkdf_from_path(dir_path, sort=True)

spark_df.show()
spark_df_sorted.show()

spark_df1.show()
spark_df_sorted1.show()

spark_df_dir.show()
spark_df_dir_sorted.show()

print(spark_df_dir.count())
print(spark_df_dir_sorted.count())

event_stream = parquet_importer.import_event_stream(file_path)
log = parquet_importer.transform_event_stream_to_event_log(event_stream)
print(log)
