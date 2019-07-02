import os
from tests.constants import INPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp

df = spark_df_imp.import_csv(os.path.join(INPUT_DATA_DIR, "receipt.csv"))
df.show(n=4)
