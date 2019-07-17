import os
from tests.constants import INPUT_DATA_DIR, OUTPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer
from pm4pyspark.dfg import calculate


df_ex = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True, inferSchema=True)
freq_tuples_ex = calculate.get_freq_tuples(df_ex)
calculate.save_dfg(df_ex, os.path.join(OUTPUT_DATA_DIR, "running-example.svg"))

df_receipt = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "receipt.csv"), header=True, inferSchema=True)
freq_tuples_receipt = calculate.get_freq_tuples(df_receipt)
calculate.save_dfg(df_receipt, os.path.join(OUTPUT_DATA_DIR, "receipt.svg"))
