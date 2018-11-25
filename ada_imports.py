import re
import os
import shutil
import pyspark
from pyspark.sql import functions
from pyspark.sql import types
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *

from pyspark import SparkContext

from datetime import datetime
from ada_const import *
from ada_context import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

if current_context == LOCAL:
    DATA_DIR = 'data_sample'
elif current_context == CLUSTER:
    DATA_DIR = 'hdfs:///datasets/gdeltv2'
