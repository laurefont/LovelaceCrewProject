from ada_const import *
from ada_context import *
from ada_imports import *
from ada_dataIO import *
from ada_dataCleaner import *

events = None
mentions = None


def main():
    """
    Computes and saves all dataframes needed for our statistics
    :return: 0 if successful
    """
    global events
    global mentions

    events = loadGDELT(EVENTS)
    mentions = loadGDELT(MENTIONS)

    events = cleanEvents(events)
    mentions = cleanMentions(mentions)



    # start, stop = get_period_mentions(mentions)
    # print('Mentions collection started on {} and stopped on {}'.format(start, stop))

    # start, stop = get_period_events_mentions(mentions)
    # print('Events mentioned in the sample of mentions took place from {} to {}'.format(start, stop))

    # start, stop = get_period_events(events)
    # print('Events recorded in the sample of events started on {} and stopped on {}'.format(start, stop))

    # Mentions, Mediatic Coverage and Mediatic Attention
    # CHANGE NUMBER OF DAYS !!!!!!!!
    mentions = restric_cov(get_delay(mentions), 60)
    # saveDataFrame(get_media_cov(mentions.select('GLOBALEVENTID')), 'get_media_cov')  # TODO: rerun

    # Confidence in our data
    # CHANGE : select the right columns !!!!!!!!!!
    # saveDataFrame(get_confidence(mentions.select('Confidence', 'GLOBALEVENTID')), 'get_confidence')  #TODO: DONE
    mentions = get_goodConfidence(mentions)
    mentions.write.mode('overwrite').parquet("mentions.parquet")
    mentions = spark.read.parquet("mentions.parquet")
    print("mentions to parquet done")


    # Origin of our data
    # CHANGE : select the right columns !!!!!!!!!!
    # saveDataFrame(get_sources(mentions.select('MentionType','GLOBALEVENTID')), 'get_sources')  #TODO: DONE
    # NEW !!!!!!!!!!!
    # saveDataFrame(get_sources_names(mentions.select('MentionSourceName')), 'get_sources_names')  # TODO: DONE

    # milestone 3
    # renamed media coverage to media attention !!!!!!!!
    # saveDataFrame(get_events_media_attention(), 'get_events_media_attention')  # TODO: faire marcher, important
    # NEW !!!!!!!
    saveDataFrame(get_events_per_country(events.select('GLOBALEVENTID', 'MonthYear_Date', 'ActionGeo_CountryCode')), 'get_events_country_time')  # TODO: run

    # Time
    # CHANGE : select the right columns !!!!!!!!!!
    # saveDataFrame(get_events_worldwide(events.select('MonthYear_Date')), 'get_events_worldwide')  #TODO: DONE
    # saveDataFrame(get_media_coverage_worldwide(mentions.select('MentionTimeDate')), 'get_media_coverage_worldwide')  #TODO: DONE

    # saveDataFrame(largest_events(mentions), 'largest_events')  #TODO: DONE
    saveDataFrame(largest_events_day_month_year(mentions), 'largest_events_day_month_year')  #TODO: voir par quel fuck ca fait tt planter

    # Geography TODO: bat les couilles en vrai
    # saveDataFrame(get_events_country(events), 'get_events_country') TODO: uncomment and watch out for black magic
    # saveDataFrame(get_media_coverage_country(events, mentions), 'get_media_coverage_country') TODO: same

    # Type of Event Bias
    # BYE BYE
    # saveDataFrame(get_goldstein_desc(events), 'get_goldstein_desc')  #TODO: DONE
    # CHANGE : : select the right columns !!!!!!!!!!
    # saveDataFrame(get_activity_byGoldstein(events.select('GoldsteinScale', 'GLOBALEVENTID')), 'get_activity_byGoldstein')  #TODO: debug empty column
    # saveDataFrame(get_cov_index(events, mentions, 'GoldsteinScale'), 'get_cov_index')  #TODO: DONE
    # saveDataFrame(get_quad(events.select('QuadClass', 'GLOBALEVENTID')), 'get_quad')  #TODO: DONE
    # saveDataFrame(get_cov_quad_relevant(events.select('GLOBALEVENTID', 'QuadClass'), mentions.select('GLOBALEVENTID')), 'get_cov_quad_relevant')  #TODO: DONE
    # saveDataFrame(get_activity_byType(events.select('EventRootCode', 'GLOBALEVENTID')), 'get_activity_byType')  #TODO: debug empty column

    # Let's now concentrate on some countries....
    print("begin worldwide")
    saveDataFrame(get_events_worldwide(events.select('MonthYear_Date')), 'get_events_worldwide')
    print("saved worldwide")
    arg = events.select('MonthYear_Date', 'ActionGeo_CountryCode')
    events_US = arg.filter(arg['ActionGeo_CountryCode'] == 'US').select('MonthYear_Date')
    events_US.write.mode('overwrite').parquet("arg.parquet")
    events_US = spark.read.parquet("arg.parquet")
    print("US events filtered and stored")
    events_US_time = get_events_worldwide(events_US)
    saveDataFrame(events_US_time, 'events_US_time')

    if True:
        return 0

    saveDataFrame(get_Goldstein(events_US.select('MonthYear_Date', 'GoldsteinScale')), 'Goldstein_US')
    mentions_US = events_US.join(mentions, 'GLOBALEVENTID')
    saveDataFrame(get_media_coverage_worldwide(mentions_US), 'mentions_US_time')

    events_SY = events.filter(events['ActionGeo_CountryCode'] == 'SY')
    mentions_SY = events_SY.join(mentions, 'GLOBALEVENTID')
    saveDataFrame(get_events_worldwide(events_SY), 'events_SY_time')
    saveDataFrame(get_media_coverage_worldwide(mentions_SY), 'mentions_SY_time')
    saveDataFrame(get_Goldstein(events_SY.select('MonthYear_Date', 'GoldsteinScale')), 'Goldstein_SY')

    events_PK = events.filter(events['ActionGeo_CountryCode'] == 'PK')
    mentions_PK = events_PK.join(mentions, 'GLOBALEVENTID')
    saveDataFrame(get_events_worldwide(events_PK), 'events_PK_time')
    saveDataFrame(get_media_coverage_worldwide(mentions_PK), 'mentions_PK_time')
    saveDataFrame(get_Goldstein(events_PK.select('MonthYear_Date', 'GoldsteinScale')), 'Goldstein_PK')

    events_AS = events.filter(events['ActionGeo_CountryCode'] == 'AS')
    mentions_AS = events_AS.join(mentions, 'GLOBALEVENTID')
    saveDataFrame(get_events_worldwide(events_AS), 'events_AS_time')
    saveDataFrame(get_media_coverage_worldwide(mentions_AS), 'mentions_AS_time')
    saveDataFrame(get_Goldstein(events_AS.select('MonthYear_Date', 'GoldsteinScale')), 'Goldstein_AS')
    saveDataFrame(get_activity_byTypeCountry(), 'get_activity_byTypeCountry')

    return 0


####################
# Data Exploration #
####################

# When were the mentions collected ?
def get_period_mentions(df_mentions):
    start = df_mentions.where(col("MentionTimeDate").isNotNull()).select('MentionTimeDate').orderBy(
        'MentionTimeDate').head()
    stop = df_mentions.select('MentionTimeDate').orderBy(desc('MentionTimeDate')).head()

    return start[0], stop[0]


# When did the events recorded in the sample of mentions take place?
def get_period_events_mentions(df_mentions):
    start = df_mentions.where(col("EventTimeDate").isNotNull()).select('EventTimeDate').orderBy('EventTimeDate').head()
    stop = df_mentions.select('EventTimeDate').orderBy(desc('EventTimeDate')).head()

    return start[0], stop[0]


# When did the recorded events take place?
def get_period_events(df_events):
    start = df_events.where(col("date").isNotNull()).select('date').orderBy('date').head()
    stop = df_events.select('date').orderBy(desc('date')).head()

    return start[0], stop[0]


######################
# Origin of our data #
######################

def get_labels(labels):
    return [x for i, x in enumerate(NEWS_SOURCES) if (i + 1) in labels]


def get_sources(df_mentions):
    sources = df_mentions.groupby('MentionType').agg(count('GLOBALEVENTID').alias('Number Mentions')).orderBy(
        'MentionType')
    return sources.select('Number Mentions', 'MentionType')


# NEW !!!!!!!!!!!
def get_sources_names(df_mentions):
    # returns the 80 most prominent media sources

    return df_mentions.groupBy('MentionSourceName').count().orderBy(desc('count')).limit(80)


##########################
# Confidence in our data #
##########################

# Getting the percentage of mentions for each confidence value
def get_confidence(df_mentions):
    '''
    total_mentions = df_mentions.count()
    get_mentions_percent = UserDefinedFunction(lambda x: x / total_mentions, DoubleType())

    confidence = df_mentions.groupby('Confidence').agg(count('GLOBALEVENTID').alias('Number Mentions')).orderBy(
        'Confidence')
    confidence = confidence.select([get_mentions_percent(column).alias('Percentage of Mentions')
                                    if column == 'Number Mentions' else column for column in confidence.columns])
    '''
    confidence = df_mentions.groupby('Confidence').count()

    return confidence


def get_goodConfidence(df_mentions):
    index = df_mentions['Confidence'] > 20
    return df_mentions[df_mentions.schema.names][index]


#####################################################
# Mentions, Mediatic Coverge and Mediatic Attention #
#####################################################

def get_delay(df_mentions):
    # Get delay between event time and mention time

    timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    timeDiff = (unix_timestamp('MentionTimeDate', format=timeFmt) - unix_timestamp('EventTimeDate', format=timeFmt))

    return df_mentions.withColumn("Mention_delay", timeDiff)


def restric_cov(df_mentions, days_threshold):
    # Narrow down mentions to 2 month posterior to event mentions

    restric_index = df_mentions['Mention_Delay'] <= days_threshold * 24 * 3600

    return df_mentions[df_mentions.schema.names][restric_index]


def get_media_cov(df_mentions):
    # Computing the mediatic coverage of each event in the mentions database

    return df_mentions.groupby('GLOBALEVENTID').agg(count('GLOBALEVENTID').alias('Number Mentions'))


########
# Time #
########


# WORLDWIDE
def get_events_worldwide(events_df):
    # format_yearmonth = UserDefinedFunction(lambda x: datetime.strptime(x, '%Y%m').strftime('%m-%Y'))

    events_worldwide = events_df.groupBy('MonthYear_Date').count().orderBy('MonthYear_Date')
    return events_worldwide


# WORLDWIDE
udf_mention1 = UserDefinedFunction(lambda x: x.strftime('%Y%m'))
udf_mention2 = UserDefinedFunction(lambda x: datetime.strptime(x, '%Y%m').strftime('%m-%Y'))


# returns the number of mentions for each month, regardless of the countries
def get_media_coverage_worldwide(mentions_df):
    mentions_Year_Month = mentions_df.select(
        [udf_mention1(column).alias('Year_Month_Mention') if column == 'MentionTimeDate' else column for column in
         mentions_df.columns])
    mentions_year_month = mentions_Year_Month.groupBy('Year_Month_Mention').count().orderBy('Year_Month_Mention')
    mentions_month_year = mentions_year_month.select(
        [udf_mention2(column).alias('Month_Year_Mention') if column == 'Year_Month_Mention' else column for column in
         mentions_year_month.columns])

    return mentions_month_year


# get the 20 events which are the most mentioned
def largest_events(df_mentions):
    return df_mentions.groupBy('GLOBALEVENTID').count().orderBy(desc('count')).limit(20)


# finds each mention of the most mentioned events
def largest_events_time(df_mentions):
    largest20_events = largest_events(df_mentions)
    ids_list = largest20_events.select('GLOBALEVENTID').collect()
    ids_array = [int(i.GLOBALEVENTID) for i in ids_list]
    return mentions.select('GLOBALEVENTID', 'MentionTimeDate').filter(col('GLOBALEVENTID').isin(ids_array))


# finds the number of mentions per month for the most mentioned events (converts to a conveniable time format)
def largest_events_month_year(df_mentions):
    tmp = largest_events_time(df_mentions)
    largest_events_Year_Month = tmp.select(
        [udf_mention1(column).alias('Year_Month_Mention') if column == 'MentionTimeDate' else column for column in
         tmp.columns])
    largest_events_year_month = largest_events_Year_Month.groupBy('Year_Month_Mention',
                                                                  'GLOBALEVENTID').count().orderBy(
        'Year_Month_Mention')
    return largest_events_year_month.select(
        [udf_mention2(column).alias('Month_Year_Mention') if column == 'Year_Month_Mention' else column for column in
         largest_events_year_month.columns])


udf_largest1 = UserDefinedFunction(lambda x: x.strftime('%Y%m%d'))
udf_largest2 = UserDefinedFunction(lambda x: datetime.strptime(x, '%Y%m%d').strftime('%d-%m-%Y'))


# finds the number of mentions per day for the most mentioned events (converts to a conveniable time format)
def largest_events_day_month_year(df_mentions):
    tmp = largest_events_time(df_mentions)
    largest_events_Year_Month = tmp.select(
        [udf_largest1(column).alias('Day_Mention') if column == 'MentionTimeDate' else column for column in
         tmp.columns])
    largest_events_year_month = largest_events_Year_Month.groupBy('Day_Mention',
                                                                  'GLOBALEVENTID').count().orderBy(
        'Day_Mention')
    return largest_events_year_month.select(
        [udf_largest2(column).alias('Day_Month_Year_Mention') if column == 'Day_Mention' else column for column in
         largest_events_year_month.columns])


#############
# Geography #
#############

def get_events_country(df_events):
    return df_events.groupBy('ActionGeo_CountryCode').agg(count('GLOBALEVENTID').alias('human_activity')).orderBy(
        'human_activity')


# returns the media coverage for each country over the 2 years
def get_media_coverage_country(events_df, mentions_df):
    # mentions per event
    mentions_count = mentions_df.groupBy('GLOBALEVENTID').count()
    mentions_count1 = mentions_count.join(events_df, 'GLOBALEVENTID')
    # mentions per country over the 2 years (= media coverage)
    country_count = mentions_count1.groupBy('ActionGeo_CountryCode').agg(sum('count').alias('media_coverage')).orderBy(
        desc('media_coverage'))
    # total number of mentions for all the events of the dataset which were recorded in the mentions dataset
    total = mentions_df.join(events, 'GLOBALEVENTID').count()
    # percentage of mentions per country over worldwide mentions over the 2 years (= mediatic attention)
    country_count = country_count.withColumn('media_attention', col('media_coverage') / total)
    country_count = country_count.withColumn('media_attention', col('media_attention') * 100)
    return country_count


######################
# Type of Event Bias #
######################

def get_goldstein_desc(df_events):
    return df_events.select('GoldsteinScale').describe()


# Get the number of events reported for each Goldstein ratio value
def get_activity_byGoldstein(df_events):
    total_event = df_events.count()
    get_events_percent = UserDefinedFunction(lambda x: x / total_event, DoubleType())

    goldstein = df_events.groupby('GoldsteinScale').agg(count('GLOBALEVENTID').alias('Number Events')).orderBy(
        'GoldsteinScale')

    return goldstein.select(
        [get_events_percent(column).alias('Fraction of Events') if column == 'Number Events' else column for column in
         goldstein.columns])


# Get the media coverage and `index` ratio for each event
def get_cov_index(df_events, df_mentions, index):
    # get_media_cov returns the number of mentions per event
    df_mentions = get_media_cov(df_mentions.select('GLOBALEVENTID')).alias('mentions')
    df_events = df_events.select(['GLOBALEVENTID', index]).alias('events')
    cov_index = df_events.join(df_mentions, df_events['GLOBALEVENTID'] == df_mentions['GLOBALEVENTID'],
                               how='left').select(['events.' + index, 'mentions.*'])
    cov_index = cov_index.where(cov_index['GLOBALEVENTID'].isNotNull())
    cov_index = cov_index.groupBy(index).agg(sum('Number Mentions').alias('Number Mentions')).sort(index)

    return cov_index


def get_class(labels):
    return [x for i, x in enumerate(QUAD_CLASSES) if (i + 1) in labels]


def get_quad(df_events):
    return df_events.groupby('QuadClass').agg(count('GLOBALEVENTID').alias('Number Events')).orderBy('QuadClass')


def get_cov_quad_relevant(df_events, df_mentions):
    df_mentions = get_media_cov(df_mentions).alias('mentions')
    df_events = df_events.alias('events')
    cov_quad = df_events.join(df_mentions, df_events['GLOBALEVENTID'] == df_mentions['GLOBALEVENTID'],
                              how='left').select(['events.QuadClass', 'mentions.*']).sort('GLOBALEVENTID')

    return cov_quad.groupby('QuadClass').agg(sum(cov_quad['Number Mentions']).alias('Number Mentions')).orderBy(
        'QuadClass')


# returns the proportion of events which are in each category of events
def get_activity_byType(df_events):
    total_event = df_events.count()
    get_events_percent = UserDefinedFunction(lambda x: x / total_event, DoubleType())

    root_type = df_events.groupby('EventRootCode').agg(count('GLOBALEVENTID').alias('Number of Events')).sort(
        'EventRootCode')
    root_type = root_type.select(
        [get_events_percent(column).alias('Percentage of Events') if column == 'Number of Events' else column for column
         in
         root_type.columns])

    return root_type


###############################################
# Let's now concentrate on some countries.... #
###############################################

# gives the average Goldstein ration per month
def get_Goldstein(df):
    format_yearmonth = UserDefinedFunction(lambda x: datetime.strptime(x, '%Y%m').strftime('%m-%Y'))
    df_Goldstein = df.groupBy('MonthYear_Date').agg(mean('GoldsteinScale').alias('av_Goldstein')).orderBy(
        'MonthYear_Date')
    return df_Goldstein.select(
        [format_yearmonth(column).alias('Month_Year') if column == 'MonthYear_Date' else column for column in
         df_Goldstein.columns])


def get_violentevents(df_events):
    violent_index = df_events['EventRootCode'] >= 18
    return df_events[violent_index]


def get_peacefullevents(df_events):
    peace_index = df_events['EventRootCode'] <= 3
    return df_events[peace_index]


def get_activity_byTypeCountry():
    violent = get_violentevents(events)
    peace = get_peacefullevents(events)
    df_events = peace.union(violent)

    total_event = df_events.count()
    get_events_percent = UserDefinedFunction(lambda x: x / total_event, DoubleType())

    count_type = df_events.groupby('ActionGeo_CountryCode', 'EventRootCode').agg(
        count('GLOBALEVENTID').alias('Number of Events')).orderBy('ActionGeo_CountryCode', 'EventRootCode')
    count_type = count_type.select(
        [get_events_percent(column).alias('Percentage of Events') if column == 'Number of Events' else column for column
         in count_type.columns])

    return count_type


###############
# milestone 3 #
###############

# ADDED THIS FUNCTION !!!!!!!
# number of events for each country for each month
def get_events_per_country(events_df):
    '''
    events_worldwide = events_df.groupBy('MonthYear_Date', 'ActionGeo_CountryCode').agg(
        count('GLOBALEVENTID').alias('Number_Events')).orderBy(['MonthYear_Date', 'Number_Events'],
                                                               ascending=[True, True])
    ret = events_worldwide.select(
        [udf_mention2(column).alias('Month_Year') if column == 'MonthYear_Date' else column for column in
         events_worldwide.columns])
    '''
    events_df.write.mode('overwrite').parquet("events_df.parquet")
    events_df = spark.read.parquet("events_df.parquet")
    print("events_df to parquet done")
    ret = events_df.groupBy('MonthYear_Date', 'ActionGeo_CountryCode').count()

    # return ret.withColumn('ActionGeo_CountryCode', isoCodes(ret.ActionGeo_CountryCode))
    return ret


# returns the number of mentions for each country for each of the biggest sources (from 4 different countries)
def mentions_biggest_sources(df_mentions, df_events, selected_sources):
    mentions_selected_sources = df_mentions.filter(col('MentionSourceName').isin(selected_sources))
    mentions_selected_sources = mentions_selected_sources.join(df_events, 'GLOBALEVENTID')
    return mentions_selected_sources.groupBy('MentionSourceName', 'ActionGeo_CountryCode').agg(
        count('GLOBALEVENTID').alias('Number_Mentions')).orderBy('MentionSourceName', 'ActionGeo_CountryCode')


# returns the media coverage for each country for each month
def get_media_cov_per_country(df_events, df_mentions):
    df = df_mentions.join(df_events, 'GLOBALEVENTID').select('GLOBALEVENTID', 'ActionGeo_CountryCode',
                                                             'MentionTimeDate')
    mentions_Year_Month = df.select(
        [udf_mention1(column).alias('Year_Month_Mention') if column == 'MentionTimeDate' else column for column in
         df.columns])
    mentions_year_month = mentions_Year_Month.groupBy('Year_Month_Mention', 'ActionGeo_CountryCode').count().orderBy(
        'Year_Month_Mention')
    mentions_month_year = mentions_year_month.select(
        [udf_mention2(column).alias('Month_Year_Mention') if column == 'Year_Month_Mention' else column for column in
         mentions_year_month.columns])
    return mentions_month_year


def get_activity_byTypeCountry_time(df_events):
    violent = get_violentevents(df_events)
    peace = get_peacefullevents(df_events)
    df = peace.union(violent)
    df_new = df.groupby('ActionGeo_CountryCode', 'MonthYear_Date', 'EventRootCode').agg(
        count('GLOBALEVENTID').alias('Number of Events')).orderBy('ActionGeo_CountryCode', 'MonthYear_Date',
                                                                  'EventRootCode')
    return df_new.select(
        [udf_mention2(column).alias('Month_Year') if column == 'MonthYear_Date' else column for column in
         df_new.columns])


def get_events_media_attention():
    df = mentions.select('GLOBALEVENTID', 'EventTimeDate', 'MentionTimeDate')
    df.createTempView('mentions')
    query = '''
        WITH simple_events AS (
            SELECT DISTINCT GLOBALEVENTID, EventTimeDate
            FROM mentions
        ),
        mentions_total AS (
            SELECT e.GLOBALEVENTID, count(m.GLOBALEVENTID) AS mentions_pool
            FROM simple_events e, mentions m
            WHERE months_between(m.MentionTimeDate, e.EventTimeDate) BETWEEN 0 AND 2
            GROUP BY e.GLOBALEVENTID
        ),
        specific_mentions AS (
            SELECT e.GLOBALEVENTID, count(m.GLOBALEVENTID) AS mentions_count
            FROM simple_events e
            INNER JOIN mentions m
                ON e.GLOBALEVENTID = m.GLOBALEVENTID
            WHERE months_between(m.MentionTimeDate, e.EventTimeDate) BETWEEN 0 AND 2
            GROUP BY e.GLOBALEVENTID
        )
        SELECT t.GLOBALEVENTID, mentions_count, mentions_pool, mentions_count / mentions_pool AS coverage
        FROM mentions_total t
        INNER JOIN specific_mentions s
            ON t.GLOBALEVENTID = s.GLOBALEVENTID
    '''
    return spark.sql(query)


if __name__ == "__main__":
    main()
