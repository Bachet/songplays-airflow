from airflow.plugins_manager import AirflowPlugin

import plugins.operators as pt
import plugins.helpers as hlp


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [pt.StageToRedshiftOperator, pt.LoadFactOperator, pt.LoadDimensionOperator, pt.DataQualityOperator]
    helpers = [hlp.SqlQueries]
