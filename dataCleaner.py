from const import *
from context import *
from imports import *


def fips2iso(fips):
    if fips in FIPS_ISO:
        return FIPS_ISO.get(fips)
    else:
        return fips


def cleanEvents(events_df):
    if events_df is null:
        return null
    isoCodes = functions.udf(fips2iso, types.StringType())
    tmp = events_df.select('GLOBALEVENTID',
                           to_date(events_raw.Day_DATE.cast('String'), 'yyyyMMdd').alias('date'),
                           'MonthYear_Date',
                           'Year_Date',
                           'FractionDate',
                           'EventRootCode',
                           'QuadClass',
                           'GoldsteinScale',
                           'AvgTone',
                           'ActionGeo_CountryCode')
    tmp = tmp.withColumn('ActionGeo_CountryCode', isoCodes(functions.col('ActionGeo_CountryCode')))
    return tmp.select('GLOBALEVENTID',
                      'date',
                      dayofmonth(events.date).alias('Day_Date'),
                      month(events.date).alias('Month_Date'),
                      'Year_Date',
                      'FractionDate',
                      'QuadClass',
                      'GoldsteinScale',
                      'AvgTone',
                      'ActionGeo_CountryCode')


def cleanMentions(mentions_df):
    if mentions_df is null:
        return null
    return mentions_df.select('GLOBALEVENTID',
                              to_timestamp(mentions_raw.EventTimeDate.cast('String'), 'yyyyMMddHHmmss').alias(
                                  'EventTimeDate'),
                              to_timestamp(mentions_raw.MentionTimeDate.cast('String'), 'yyyyMMddHHmmss').alias(
                                  'MentionTimeDate'),
                              'MentionType',
                              'Confidence')
