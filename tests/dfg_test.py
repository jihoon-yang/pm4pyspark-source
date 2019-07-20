import os
from pm4pyspark.dfg import calculate
from tests.constants import INPUT_DATA_DIR, OUTPUT_DATA_DIR
from pm4pyspark.importer.csv import spark_df_imp as importer


event_stream_ex = importer.import_event_stream(os.path.join(INPUT_DATA_DIR, "running-example.csv"), parameters={"header": True})
log_ex = importer.transform_event_stream_to_event_log(event_stream_ex)
df_ex = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True, inferSchema=True)
freq_tuples_ex = calculate.get_freq_tuples(df_ex)
calculate.save_dfg(df_ex, log=log_ex, path=os.path.join(OUTPUT_DATA_DIR, "running-example_freq.svg"))
calculate.save_dfg(df_ex, log=log_ex, variant="performance", path=os.path.join(OUTPUT_DATA_DIR, "running-example_perf.svg"))

event_stream_receipt = importer.import_event_stream(os.path.join(INPUT_DATA_DIR, "receipt.csv"), parameters={"header": True})
log_receipt = importer.transform_event_stream_to_event_log(event_stream_receipt)
df_receipt = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "receipt.csv"), header=True, inferSchema=True)
freq_tuples_receipt = calculate.get_freq_tuples(df_receipt)
calculate.save_dfg(df_receipt, log=log_receipt, path=os.path.join(OUTPUT_DATA_DIR, "receipt_freq.svg"))
calculate.save_dfg(df_receipt, log=log_receipt, variant="performance", path=os.path.join(OUTPUT_DATA_DIR, "receipt_perf.svg"))
