# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import col,when
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import LongType, ArrayType, DoubleType, BooleanType

import xgboost as xgb
from xgboost import XGBClassifier

import mlflow
import mlflow.xgboost
import mlflow.pyspark.ml
from mlflow.models.signature import infer_signature
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression,RandomForestClassifier,GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from sklearn.metrics import classification_report, confusion_matrix,precision_recall_fscore_support, precision_recall_curve
from sklearn.metrics import accuracy_score,average_precision_score,precision_score,confusion_matrix,recall_score,roc_curve,auc,f1_score
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer

import datetime
import time
import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sklearn 
import seaborn as sns
from tqdm import tqdm
# import backoff

import slack
from slack import WebClient

# COMMAND ----------

time.time()

# COMMAND ----------

_redshift_config = {
    'host' : 'dream11-segment-2.cd2mebwblp0z.us-east-1.redshift.amazonaws.com',
    'user' : 'dsuser',
    'port' : '5439',
    'password' : 'Dd123dLn4rHDd7Y8',
    'dbname' : 'segment'
}

_archive_data_config = {
    'host' : 'archive-new-db-0512-cluster.cluster-ro-cefqnpdgdxcd.us-east-1.rds.amazonaws.com',
    'user' : 'dsuser',
    'port' : '3306',
    'password' : 'fJn@x>u!LT4$WL',
    'dbname' : 'dbd11live29june15',
    'driver' : 'org.mariadb.jdbc.Driver'
}

# COMMAND ----------

base_s3_path = "s3a://d11-data-lake/data-science/fpv/fpv_edge_prediction/"
slack_channel = "testing_alerts"

# COMMAND ----------

lookback_window_hours = 24

last_ts = '2021-09-14 01:16:40.000000'
max_ts = '2021-09-16 00:59:04.000000'

last_ts = datetime.datetime.strptime(last_ts, '%Y-%m-%d %H:%M:%S.%f')
max_ts = datetime.datetime.strptime(max_ts, '%Y-%m-%d %H:%M:%S.%f')


    
# update_query = '''insert into ds_fpv.fpv_multiple_id_timestamps (jobname, last_timestamp)
#                   values('fpv-edge-prediciton_prev_ts','2021-09-07 04:15:00.964033')      '''
# spark.sql(update_query)

# COMMAND ----------

job_name = 'fpv-edge-prediciton'

# last_ts_df = sqlContext.sql("""select jobname, last_timestamp as last_timestamp from ds_fpv.fpv_multiple_id_timestamps where jobname = '{0}'""".format(job_name))
# last_ts = last_ts_df.collect()[0][1]

# max_ts_df = sqlContext.sql("""select jobname, last_timestamp as last_timestamp from ds_fpv.fpv_multiple_id_timestamps where jobname = '{0}'""".format('send_file_slack'))
# max_ts = max_ts_df.collect()[0][1]

# COMMAND ----------

print('last_ts =',last_ts)
print('max_ts =',max_ts)
print('Look back window =',lookback_window_hours)
print('base_s3_path =',base_s3_path)
print('Slack channel =',slack_channel)

# COMMAND ----------


