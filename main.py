import re

import os
import shutil

import pyspark
from pyspark.sql import *
from pyspark.sql.functions import unix_timestamp, udf, to_date
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


OUT_DIR = 'output'
DATA_DIR = 'hdfs:///datasets/gdeltv2'


def rmOutDir():
    shutil.rmtree(OUT_DIR, True)
    shutil.rmtree("foo", True)
    shutil.rmtree("bar", True)


def save(df):
    df.write.mode('overwrite').csv('df_save')


def main():
    rmOutDir()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    sc = spark.sparkContext

    # exports = spark.read.csv(os.path.join(DATA_DIR, "*.export.CSV"), sep="\t")
    exports = spark.read.csv(os.path.join(DATA_DIR, "20150530104500.export.CSV"), sep="\t", schema=EVENTS_SCHEMA)

    save(exports)

    print("hello world")
    return 0


GKG_SCHEMA = StructType([
    StructField("GKGRECORDID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("SourceCollectionIdentifier", StringType(), True),
    StructField("SourceCommonName", StringType(), True),
    StructField("DocumentIdentifier", StringType(), True),
    StructField("Counts", StringType(), True),
    StructField("V2Counts", StringType(), True),
    StructField("Themes", StringType(), True),
    StructField("V2Themes", StringType(), True),
    StructField("Locations", StringType(), True),
    StructField("V2Locations", StringType(), True),
    StructField("Persons", StringType(), True),
    StructField("V2Persons", StringType(), True),
    StructField("Organizations", StringType(), True),
    StructField("V2Organizations", StringType(), True),
    StructField("V2Tone", StringType(), True),
    StructField("Dates", StringType(), True),
    StructField("GCAM", StringType(), True),
    StructField("SharingImage", StringType(), True),
    StructField("RelatedImages", StringType(), True),
    StructField("SocialImageEmbeds", StringType(), True),
    StructField("SocialVideoEmbeds", StringType(), True),
    StructField("Quotations", StringType(), True),
    StructField("AllNames", StringType(), True),
    StructField("Amounts", StringType(), True),
    StructField("TranslationInfo", StringType(), True),
    StructField("Extras", StringType(), True)
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

if __name__ == "__main__":
    main()
