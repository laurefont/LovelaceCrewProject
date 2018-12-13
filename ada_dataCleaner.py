from ada_const import *
from ada_context import *
from ada_imports import *


def fips2iso(fips):
    if fips in FIPS_ISO:
        return FIPS_ISO.get(fips)
    else:
        return fips


isoCodes = functions.udf(fips2iso, types.StringType())


def cleanEvents(events_df):
    if events_df is None:
        return None
    global isoCodes
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
                           'ActionGeo_CountryCode')
    # ISO country codes conversion :
    #   df.select(
    #       'first_col',
    # ->    isoCodes(functions.col('ActionGeo_CountryCode')).alias('ActionGeo_CountryCode',
    #       'another_col'
    #   )

    # first mention recording
    # first_record = datetime.strptime('20150218', '%Y%m%d')
    # last event considered to have 2 months coverage for every event
    # last_considered_events = datetime.strptime('20170921', '%Y%m%d')

    ## SMALL DATASET ##
    first_record = datetime.strptime('20160101', '%Y%m%d')
    last_considered_events = datetime.strptime('20161231', '%Y%m%d')
    ###################

    tmp = tmp.filter(tmp['date'] >= first_record)
    tmp = tmp.filter(tmp['date'] <= last_considered_events)
    return tmp.select('GLOBALEVENTID',
                      'date',
                      dayofmonth(tmp.date).alias('Day_Date'),
                      month(tmp.date).alias('Month_Date'),
                      'Year_Date',
                      'MonthYear_Date',
                      'FractionDate',
                      'EventCode',
                      'EventRootCode',
                      'QuadClass',
                      'GoldsteinScale',
                      'AvgTone',
                      'ActionGeo_CountryCode')


def cleanMentions(mentions_df):
    if mentions_df is None:
        return None

    ## SMALL DATASET ##
    first_record = datetime.strptime('20160101', '%Y%m%d')
    last_considered_events = datetime.strptime('20161231', '%Y%m%d')
    ###################

    tmp = mentions_df.select('GLOBALEVENTID',
                             to_timestamp(mentions_df.EventTimeDate.cast('String'), 'yyyyMMddHHmmss').alias(
                                 'EventTimeDate'),
                             to_timestamp(mentions_df.MentionTimeDate.cast('String'), 'yyyyMMddHHmmss').alias(
                                 'MentionTimeDate'),
                             'MentionType',
                             'Confidence',
                             'MentionSourceName')

    tmp = tmp.filter(tmp['MentionTimeDate'] >= first_record)
    tmp = tmp.filter(tmp['MentionTimeDate'] <= last_considered_events)
    return tmp
