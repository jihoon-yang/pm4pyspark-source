import os
import time

from tests.constants import INPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as csv_importer

file_path = os.path.join(INPUT_DATA_DIR, "running-example.csv")
file_path2 = os.path.join(INPUT_DATA_DIR, "receipt.csv")


spark_df_wo_timeconversion = csv_importer.import_sparkdf_from_path_wo_timeconversion(file_path, header=True)
spark_df = csv_importer.import_sparkdf_from_path(file_path, header=True, inferSchema=True)
spark_df_sorted = csv_importer.import_sparkdf_from_path(file_path, header=True, sort=True)

spark_df_wo_timeconversion1 = csv_importer.import_sparkdf_from_path_wo_timeconversion(file_path2, header=True)
spark_df1 = csv_importer.import_sparkdf_from_path(file_path2, header=True)
spark_df_sorted1 = csv_importer.import_sparkdf_from_path(file_path2, header=True, sort=True)

spark_df_wo_timeconversion.show(truncate=False)
spark_df.show()
spark_df_sorted.show()

spark_df_wo_timeconversion1.show(truncate=False)
spark_df1.show()
spark_df_sorted1.show()

event_stream = csv_importer.import_event_stream(file_path, header=True)
log = csv_importer.transform_event_stream_to_event_log(event_stream)
print(log)
