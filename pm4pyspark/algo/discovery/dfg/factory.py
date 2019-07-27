from pm4py import util as pmutil
from pm4py.objects.log.util import general as log_util
from pm4py.objects.log.util import xes as xes_util

from pm4pyspark.dfg import df_statistics
from pm4pyspark.importer.csv import spark_df_imp as importer



def apply(df, parameters=None, variant="frequency"):
    """Calculates DFG graph (frequency or performance) starting from the Spark DataFrame
    """

    if parameters is None:
        parameters = {}
    if pmutil.constants.PARAMETER_CONSTANT_ACTIVITY_KEY not in parameters:
        parameters[pmutil.constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = xes_util.DEFAULT_NAME_KEY
    if pmutil.constants.PARAMETER_CONSTANT_TIMESTAMP_KEY not in parameters:
        parameters[pmutil.constants.PARAMETER_CONSTANT_TIMESTAMP_KEY] = xes_util.DEFAULT_TIMESTAMP_KEY
    if pmutil.constants.PARAMETER_CONSTANT_CASEID_KEY not in parameters:
        parameters[pmutil.constants.PARAMETER_CONSTANT_CASEID_KEY] = log_util.CASE_ATTRIBUTE_GLUE
    df = importer.convert_timestamp_to_utc_in_df(df, timest_columns=[
        parameters[pmutil.constants.PARAMETER_CONSTANT_TIMESTAMP_KEY]])
    dfg_frequency, dfg_performance = df_statistics.get_dfg_graph(df, measure="both",
                                                                 activity_key=parameters[
                                                                    pmutil.constants.PARAMETER_CONSTANT_ACTIVITY_KEY],
                                                                 timestamp_key=parameters[
                                                                    pmutil.constants.PARAMETER_CONSTANT_TIMESTAMP_KEY],
                                                                 case_id_glue=parameters[
                                                                    pmutil.constants.PARAMETER_CONSTANT_CASEID_KEY])
    if variant == "frequency":
        return dfg_frequency
    else:
        return dfg_performance
