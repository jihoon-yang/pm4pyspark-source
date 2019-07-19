import os
import time
from tests.constants import INPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer
from pyspark.sql import SparkSession
import pandas as pd


spark_df_wo_timeconversion = importer.import_sparkdf_from_path_wo_timeconversion(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True)
spark_df = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True)
spark_df_sorted = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True, sort=True)

spark_df_wo_timeconversion1 = importer.import_sparkdf_from_path_wo_timeconversion(os.path.join(INPUT_DATA_DIR, "receipt.csv"), header=True)
spark_df1 = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "receipt.csv"), header=True)
spark_df_sorted1 = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "receipt.csv"), header=True, sort=True)

spark_df_wo_timeconversion.show(truncate=False)
spark_df.show()
spark_df_sorted.show()

spark_df_wo_timeconversion1.show(truncate=False)
spark_df1.show()
spark_df_sorted1.show()

'''
spark = SparkSession.builder.getOrCreate()
start_time = time.time()
spark_df = spark.read.csv(os.path.join(INPUT_DATA_DIR, "receipt.csv"), header=True, inferSchema=True)
print('Importing Spark DataFrame took {} seconds.'.format(time.time() - start_time))


start_time = time.time()
panda_df = pd.read_csv(os.path.join(INPUT_DATA_DIR, "receipt.csv"))
print('Importing Pandas DataFrame took {} seconds.'.format(time.time() - start_time))
'''
