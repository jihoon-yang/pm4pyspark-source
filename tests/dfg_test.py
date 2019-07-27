import os
from tests.constants import INPUT_DATA_DIR, OUTPUT_DATA_DIR
from pm4py.visualization.dfg import factory as dfg_vis_factory
from pm4pyspark.importer.csv import spark_df_imp as importer
from pm4pyspark.algo.discovery.dfg import factory as dfg_factory


parameters = {"format":"svg"}


event_stream_ex = importer.import_event_stream(os.path.join(INPUT_DATA_DIR, "running-example.csv"), parameters={"header": True})
log_ex = importer.transform_event_stream_to_event_log(event_stream_ex)
df_ex = importer.import_sparkdf_from_path(os.path.join(INPUT_DATA_DIR, "running-example.csv"), header=True, inferSchema=True)

dfg_freq = dfg_factory.apply(df_ex)
gviz_freq = dfg_vis_factory.apply(dfg_freq, log=log_ex, parameters=parameters, variant="frequency")
dfg_vis_factory.save(gviz_freq, os.path.join(OUTPUT_DATA_DIR, "running-example_freq.svg"))

dfg_perf = dfg_factory.apply(df_ex, variant="performance")
gviz_perf = dfg_vis_factory.apply(dfg_perf, log=log_ex, parameters=parameters, variant="performance")
dfg_vis_factory.save(gviz_perf, os.path.join(OUTPUT_DATA_DIR, "running-example_perf.svg"))
