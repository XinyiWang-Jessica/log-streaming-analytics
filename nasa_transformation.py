from definitions import *
import glob
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns


def zip_log_to_df(input_file_path, spark):
    # read .gz files in the given directory
    raw_data_files = glob.glob(input_file_path + '*.gz')
    # parse the log file
    base_df = spark.read.text(raw_data_files)
    # convert it to rdd
    # base_df_rdd = base_df.rdd
    return base_df

def split_to_df(df, ts_pattern):
    logs_df = df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
    return logs_df

def count_null(col_name):
    return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)

def count_null_col(df):
    col = df.columns
    exprs = [count_null(col_name) for col_name in col]
    return df.agg(*exprs)


def fill_null_values(df):
    df = df[df['status'].isNotNull()] 
    return df.na.fill({'content_size': 0})

def parse_clf_time(text):
    """ Convert Common Log time format into a Python datetime object
    Args:
        text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(text[7:11]),
      month_map[text[3:6]],
      int(text[0:2]),
      int(text[12:14]),
      int(text[15:17]),
      int(text[18:20])
    )

def month_to_num(df):
    udf_parse_time = udf(parse_clf_time)
    df = df.select('host', \
                   udf_parse_time(df['timestamp']).alias('timestamp'), \
                   'method', 'endpoint', 'protocol', 'status', 'content_size')
    return df
    

def format_timestamp(df):
    udf_parse_time = udf(parse_clf_time)
    df = df.select('*',
            df['timestamp'].cast('timestamp').alias('time')).drop('timestamp')
    return df


def plot_status(df):
    status_freq_df = (df
                     .groupBy('status')
                     .count()
                     .sort('status')
                     .cache())
    status_freq_pd_df = (status_freq_df
                         .toPandas()
                         .sort_values(by=['count'],
                                      ascending=False))
    sns.catplot(x='status', y='count', data=status_freq_pd_df, 
            kind='bar', order=status_freq_pd_df['status'])
    return status_freq_df


def plot_log_status(df):
    status_freq_df = (df
                     .groupBy('status')
                     .count()
                     .sort('status')
                     .cache())
    log_freq_df = status_freq_df.withColumn('log(count)', F.log(status_freq_df['count']))
    log_freq_pd_df = (log_freq_df
                         .toPandas()
                         .sort_values(by=['log(count)'],
                                      ascending=False))
    sns.catplot(x='status', y='log(count)', data=log_freq_pd_df, 
            kind='bar', order=log_freq_pd_df['status'])
    return status_freq_df

def top_host(df, n):
    host_sum_df =(df
               .groupBy('host')
               .count()
               .sort('count', ascending=False).limit(n))
    return host_sum_df

def top_endpoint(df, n):
    endpoint_sum_df =(df
               .groupBy('endpoint')
               .count()
               .sort('count', ascending=False).limit(n))
    return endpoint_sum_df


def top_error_endpoint(df, n):
    not200_df = (df
               .filter(df['status'] != 200))

    error_endpoints_freq_df = (not200_df
                               .groupBy('endpoint')
                               .count()
                               .sort('count', ascending=False)
                               .limit(n)
                          )
    return error_endpoints_freq_df

def parse_day_of_week(num):
    return week_map[num]

def top_endpoint_by_day(df):
    # convert time stamp to day of week
    endpoint_day_df = df.select(df.endpoint, 
                             F.dayofweek('time').alias('weekday'))
    # group by weekday and the endpoint, aggregate by counts
    endpoint_freq_df = (endpoint_day_df
                     .groupBy(['weekday', 'endpoint'])
                     .count()
                     .sort("weekday"))
    # use window function to obtain the top endpoint of each day of week
    window = Window.partitionBy("weekday").orderBy(col("count").desc())
    ranked_df = endpoint_freq_df.withColumn("rank", F.rank().over(window))
    result = ranked_df.filter(F.col("rank") == 1).select("weekday", "endpoint", "count")
    # convert day as an integer to name of the day
    udf_parse_day = udf(parse_day_of_week)
    result = result.select('*', udf_parse_day(result['weekday']).alias('Day in a week')).drop('weekday')
    return result

