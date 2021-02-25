import pyspark
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark import SparkContext, StorageLevel
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import os , sys
import multiprocessing as mp
import uuid

def session():
    from IPython.core.interactiveshell import InteractiveShell
    InteractiveShell.ast_node_interactivity = "all"
    # prevent error on rerunning if session is still alive
    if 'sc' in globals():
        sc.stop()

    # most configuration of the sparksession is done for you in the back
    application_name = 'testing'

    conf = pyspark.SparkConf().setAppName(application_name).setMaster('local[*]')


    sparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = sparkSession.sparkContext
    return sparkSession, sc
        