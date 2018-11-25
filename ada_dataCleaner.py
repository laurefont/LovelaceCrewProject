from ada_const import *
from ada_context import *
from ada_imports import *


def fips2iso(fips):
    if fips in FIPS_ISO:
        return FIPS_ISO.get(fips)
    else:
        return fips


def cleanEvents(events_df):
    if events_df is None:
        return None
    isoCodes = functions.udf(fips2iso, types.StringType())
    tmp = events_df.select('GLOBALEVENTID',
                           to_date(events_df.Day_DATE.cast('String'), 'yyyyMMdd').alias('date'),
                           'MonthYear_Date',
                           'Year_Date',
                           'FractionDate',
                           'EventCode',
                           'EventRootCode',
                           'QuadClass',
                           round(events_df.GoldsteinScale, 2).alias('GoldsteinScale'),
                           'AvgTone',
                           isoCodes(functions.col('ActionGeo_CountryCode')).alias('ActionGeo_CountryCode'))
    return tmp.select('GLOBALEVENTID',
                      'date',
                      dayofmonth(tmp.date).alias('Day_Date'),
                      month(tmp.date).alias('Month_Date'),
                      'Year_Date',
                      'MonthYear_Date',
                      'FractionDate',
                      'QuadClass',
                      'GoldsteinScale',
                      'AvgTone',
                      'ActionGeo_CountryCode')


def cleanMentions(mentions_df):
    if mentions_df is None:
        return None
    return mentions_df.select('GLOBALEVENTID',
                              to_timestamp(mentions_df.EventTimeDate.cast('String'), 'yyyyMMddHHmmss').alias(
                                  'EventTimeDate'),
                              to_timestamp(mentions_df.MentionTimeDate.cast('String'), 'yyyyMMddHHmmss').alias(
                                  'MentionTimeDate'),
                              'MentionType',
                              'Confidence',
                              'MentionSourceName')
