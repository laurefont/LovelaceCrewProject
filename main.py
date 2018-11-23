import re

import os
import shutil

import pyspark
from pyspark.sql import *
from pyspark.sql.functions import unix_timestamp, udf, to_date, to_timestamp
from pyspark.sql.types import *
from datetime import datetime

EVENTS_SCHEMA = StructType([
    StructField("GLOBALEVENTID", LongType(), True),
    StructField("Day_DATE", StringType(), True),
    StructField("MonthYear_Date", StringType(), True),
    StructField("Year_Date", StringType(), True),
    StructField("FractionDate", FloatType(), True),
    StructField("Actor1Code", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor1CountryCode", StringType(), True),
    StructField("Actor1KnownGroupCode", StringType(), True),
    StructField("Actor1EthnicCode", StringType(), True),
    StructField("Actor1Religion1Code", StringType(), True),
    StructField("Actor1Religion2Code", StringType(), True),
    StructField("Actor1Type1Code", StringType(), True),
    StructField("Actor1Type2Code", StringType(), True),
    StructField("Actor1Type3Code", StringType(), True),
    StructField("Actor2Code", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("Actor2CountryCode", StringType(), True),
    StructField("Actor2KnownGroupCode", StringType(), True),
    StructField("Actor2EthnicCode", StringType(), True),
    StructField("Actor2Religion1Code", StringType(), True),
    StructField("Actor2Religion2Code", StringType(), True),
    StructField("Actor2Type1Code", StringType(), True),
    StructField("Actor2Type2Code", StringType(), True),
    StructField("Actor2Type3Code", StringType(), True),
    StructField("IsRootEvent", LongType(), True),
    StructField("EventCode", StringType(), True),
    StructField("EventBaseCode", StringType(), True),
    StructField("EventRootCode", StringType(), True),
    StructField("QuadClass", LongType(), True),
    StructField("GoldsteinScale", FloatType(), True),
    StructField("NumMentions", LongType(), True),
    StructField("NumSources", LongType(), True),
    StructField("NumArticles", LongType(), True),
    StructField("AvgTone", FloatType(), True),
    StructField("Actor1Geo_Type", LongType(), True),
    StructField("Actor1Geo_FullName", StringType(), True),
    StructField("Actor1Geo_CountryCode", StringType(), True),
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    StructField("Actor1Geo_ADM2Code", StringType(), True),
    StructField("Actor1Geo_Lat", FloatType(), True),
    StructField("Actor1Geo_Long", FloatType(), True),
    StructField("Actor1Geo_FeatureID", StringType(), True),
    StructField("Actor2Geo_Type", LongType(), True),
    StructField("Actor2Geo_FullName", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True),
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_ADM2Code", StringType(), True),
    StructField("Actor2Geo_Lat", FloatType(), True),
    StructField("Actor2Geo_Long", FloatType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),
    StructField("ActionGeo_Type", LongType(), True),
    StructField("ActionGeo_FullName", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_ADM2Code", StringType(), True),
    StructField("ActionGeo_Lat", FloatType(), True),
    StructField("ActionGeo_Long", FloatType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),
    StructField("DATEADDED", LongType(), True),
    StructField("SOURCEURL", StringType(), True)
])

MENTIONS_SCHEMA = StructType([
    StructField("GLOBALEVENTID", LongType(), True),
    StructField("EventTimeDate", LongType(), True),
    StructField("MentionTimeDate", LongType(), True),
    StructField("MentionType", LongType(), True),
    StructField("MentionSourceName", StringType(), True),
    StructField("MentionIdentifier", StringType(), True),
    StructField("SentenceID", LongType(), True),
    StructField("Actor1CharOffset", LongType(), True),
    StructField("Actor2CharOffset", LongType(), True),
    StructField("ActionCharOffset", LongType(), True),
    StructField("InRawText", LongType(), True),
    StructField("Confidence", LongType(), True),
    StructField("MentionDocLen", LongType(), True),
    StructField("MentionDocTone", FloatType(), True),
    StructField("MentionDocTranslationInfo", StringType(), True),
    StructField("Extras", StringType(), True)
])

OUT_DIR = 'output'
DATA_DIR = 'hdfs:///datasets/gdeltv2'


def save(df):
    df.repartition(1).write.mode('overwrite').csv('df_save')


def main():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    sc = spark.sparkContext

    events_raw = spark.read.csv(os.path.join(DATA_DIR, "*.export.CSV"), sep="\t", schema=EVENTS_SCHEMA)
    mentions_raw = spark.read.csv(os.path.join(DATA_DIR, "*.mentions.CSV"), sep="\t", schema=MENTIONS_SCHEMA)
    # exports = spark.read.csv(os.path.join(DATA_DIR, "20150530104500.export.CSV"), sep="\t", schema=EVENTS_SCHEMA)

    events = events_raw.select('GLOBALEVENTID',
                               to_date(events_raw.Day_DATE.cast('String'), 'yyyyMMdd').alias('Day_DATE'),
                               'MonthYear_Date',
                               'Year_Date',
                               'FractionDate',
                               'Actor1CountryCode',
                               'Actor2CountryCode',
                               'QuadClass',
                               'GoldsteinScale',
                               'AvgTone',
                               'Actor1Geo_CountryCode',
                               'Actor2Geo_CountryCode')

    mentions = mentions_raw.select('GLOBALEVENTID',
                                   to_timestamp(mentions_raw.EventTimeDate.cast('String'), 'yyyyMMddHHmmss').alias(
                                       'EventTimeDate'),
                                   to_timestamp(mentions_raw.MentionTimeDate.cast('String'), 'yyyyMMddHHmmss').alias(
                                       'MentionTimeDate'),
                                   'MentionType',
                                   'Confidence')

    save(events)
    save(mentions)

    print("finished <3")
    return 0


if __name__ == "__main__":
    main()
