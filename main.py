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

    start, stop = get_period_mentions(mentions)
    print('Mentions collection started on {} and stopped on {}'.format(start, stop))

    start, stop = get_period_events_mentions(mentions)
    print('Events mentioned in the sample of mentions took place from {} to {}'.format(start, stop))

    start, stop = get_period_events(events)
    print('Events recorded in the sample of events started on {} and stopped on {}'.format(start, stop))

    # Confidence in our data and 2month delay
    saveDataFrame(get_confidence(mentions.select('Confidence', 'GLOBALEVENTID')), 'get_confidence')  # TODO: DONE 2Y
    mentions = restric_cov(get_delay(mentions), 60)
    mentions = get_goodConfidence(mentions)
    mentions.write.mode('overwrite').parquet("mentions.parquet")
    mentions = spark.read.parquet("mentions.parquet")
    print("mentions to parquet done")
    events.write.mode('overwrite').parquet("events.parquet")
    events = spark.read.parquet("events.parquet")
    print("events to parquet done")

    # Mentions, Mediatic Coverage and Mediatic Attention
    saveDataFrame(get_media_cov(mentions.select('GLOBALEVENTID'), events), 'get_media_cov')  #TODO: DONE 2Y

    # Origin of our data
    saveDataFrame(get_sources(mentions.select('MentionType', 'GLOBALEVENTID')), 'get_sources')  # TODO: DONE 2Y
    saveDataFrame(get_sources_names(mentions.select('MentionSourceName')), 'get_sources_names')  # TODO: DONE 2Y
    print(get_sources_number(mentions.select('MentionSourceName')))

    # milestone 3
    saveDataFrame(get_events_per_country(events.select('GLOBALEVENTID', 'MonthYear_Date', 'ActionGeo_CountryCode')),
                  'get_events_country_time')  # TODO: DONE 4M
    saveDataFrame(get_activity_byTypeCountry_time(events), 'get_activity_byTypeCountry_time')
    saveDataFrame(get_media_cov_per_country(events, mentions), 'get_media_cov_per_country')

    # Time
    saveDataFrame(get_events_worldwide(events.select('MonthYear_Date')), 'get_events_worldwide')  # TODO: DONE
    saveDataFrame(get_media_coverage_worldwide(mentions.select('GLOBALEVENTID', 'MentionTimeDate')),
                  'get_media_coverage_worldwide')  # TODO: DONE

    saveDataFrame(largest_events(mentions), 'largest_events')  # TODO: DONE
    saveDataFrame(largest_events_day_month_year(mentions), 'largest_events_day_month_year')  # TODO: DONE 4M

    # Type of Event Bias
    saveDataFrame(get_activity_byType(events.select('EventRootCode', 'GLOBALEVENTID')), 'get_activity_byType')  #TODO: debug empty column

    # Let's now concentrate on some countries....
    arg = events.select('GLOBALEVENTID', 'MonthYear_Date', 'GoldsteinScale', 'ActionGeo_CountryCode')

    events_US = arg.filter(arg['ActionGeo_CountryCode'] == 'US').select('GLOBALEVENTID', 'MonthYear_Date',
                                                                        'GoldsteinScale')
    events_US.write.mode('overwrite').parquet("arg.parquet")
    events_US = spark.read.parquet("arg.parquet")
    print("US events filtered and stored")
    events_US_time = get_events_worldwide(events_US)
    saveDataFrame(events_US_time, 'events_US_time')  # TODO: DONE 2Y
    saveDataFrame(get_Goldstein(events_US.select('MonthYear_Date', 'GoldsteinScale')), 'Goldstein_US')
    mentions_US = events_US.join(mentions.select('GLOBALEVENTID', 'MentionTimeDate'), 'GLOBALEVENTID')
    mentions_US.write.mode('overwrite').parquet("arg2.parquet")
    mentions_US = spark.read.parquet("arg2.parquet")
    print("US mentions filtered and stored")
    saveDataFrame(get_media_coverage_worldwide(mentions_US), 'mentions_US_time')

    events_AS = arg.filter(arg['ActionGeo_CountryCode'] == 'AS').select('GLOBALEVENTID', 'MonthYear_Date',
                                                                        'GoldsteinScale')
    events_AS.write.mode('overwrite').parquet("arg.parquet")
    events_AS = spark.read.parquet("arg.parquet")
    print("AS events filtered and stored")
    events_AS_time = get_events_worldwide(events_AS)
    saveDataFrame(events_AS_time, 'events_AS_time')  # TODO: DONE 2Y
    saveDataFrame(get_Goldstein(events_AS.select('MonthYear_Date', 'GoldsteinScale')), 'Goldstein_AS')
    mentions_AS = events_AS.join(mentions.select('GLOBALEVENTID', 'MentionTimeDate'), 'GLOBALEVENTID')
    mentions_AS.write.mode('overwrite').parquet("arg2.parquet")
    mentions_AS = spark.read.parquet("arg2.parquet")
    print("AS mentions filtered and stored")
    saveDataFrame(get_media_coverage_worldwide(mentions_AS), 'mentions_AS_time')

    events_PK = arg.filter(arg['ActionGeo_CountryCode'] == 'PK').select('GLOBALEVENTID', 'MonthYear_Date',
                                                                        'GoldsteinScale')
    events_PK.write.mode('overwrite').parquet("arg.parquet")
    events_PK = spark.read.parquet("arg.parquet")
    print("PK events filtered and stored")
    events_PK_time = get_events_worldwide(events_PK)
    saveDataFrame(events_PK_time, 'events_PK_time')  # TODO: DONE 2Y
    saveDataFrame(get_Goldstein(events_PK.select('MonthYear_Date', 'GoldsteinScale')), 'Goldstein_PK')
    mentions_PK = events_PK.join(mentions.select('GLOBALEVENTID', 'MentionTimeDate'), 'GLOBALEVENTID')
    mentions_PK.write.mode('overwrite').parquet("arg2.parquet")
    mentions_PK = spark.read.parquet("arg2.parquet")
    print("PK mentions filtered and stored")
    saveDataFrame(get_media_coverage_worldwide(mentions_PK), 'mentions_PK_time')

    events_SY = arg.filter(arg['ActionGeo_CountryCode'] == 'SY').select('GLOBALEVENTID', 'MonthYear_Date',
                                                                        'GoldsteinScale')
    events_SY.write.mode('overwrite').parquet("arg.parquet")
    events_SY = spark.read.parquet("arg.parquet")
    print("SY events filtered and stored")
    events_SY_time = get_events_worldwide(events_SY)
    saveDataFrame(events_SY_time, 'events_SY_time')  # TODO: DONE 2Y
    saveDataFrame(get_Goldstein(events_SY.select('MonthYear_Date', 'GoldsteinScale')), 'Goldstein_SY')
    mentions_SY = events_SY.join(mentions.select('GLOBALEVENTID', 'MentionTimeDate'), 'GLOBALEVENTID')
    mentions_SY.write.mode('overwrite').parquet("arg2.parquet")
    mentions_SY = spark.read.parquet("arg2.parquet")
    print("SY mentions filtered and stored")
    saveDataFrame(get_media_coverage_worldwide(mentions_SY), 'mentions_SY_time')


    ###########
    #  FINAL  #
    ###########

    biggest_sources_selection = list(
        ['washingtonpost.com', 'theguardian.com', 'france24.com', 'indiatimes.com', 'onlinenigeria.com'])
    saveDataFrame(mentions_biggest_sources1(mentions.select('MentionSourceName', 'GLOBALEVENTID'),
                                            events.select('GLOBALEVENTID', 'ActionGeo_CountryCode'),
                                            biggest_sources_selection), 'countries_mentions_biggest_sources1')

    saveDataFrame(mentions_biggest_sources2(mentions.select('MentionSourceName', 'GLOBALEVENTID'),
                                            events.select('GLOBALEVENTID', 'ActionGeo_CountryCode'),
                                            biggest_sources_selection), 'mentions_biggest_sources2')

    events_spe_countries = events.select('ActionGeo_CountryCode', 'GLOBALEVENTID', 'EventRootCode').filter(
        col('ActionGeo_CountryCode').isin(list(['US', 'AS', 'SY', 'PK'])))
    events_spe_countries.write.mode('overwrite').parquet("arg.parquet")
    events_spe_countries = spark.read.parquet("arg.parquet")
    saveDataFrame(num_violent_pacif(events_spe_countries, mentions.select('GLOBALEVENTID')), 'num_violent_pacif')
    saveDataFrame(get_av_media_coverage_country_type(mentions, events_spe_countries),
                  'get_av_media_coverage_country_type')
    saveDataFrame(men_per_src_per_country(events.select('ActionGeo_CountryCode', 'GLOBALEVENTID'), mentions.select('GLOBALEVENTID', 'MentionSourceName')), 'men_per_src_per_country')

    saveDataFrame(get_events_media_attention(), 'get_events_media_attention')

    print("At this stage, the cluster should have run for at least 10^308 times the age of Universe.")
    print("42")

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


def get_sources_names(df_mentions):
    """
    :return: 1000 most prominent media sources
    :type df_mentions: DataFrame
    :rtype: DataFrame
    """
    return df_mentions.groupBy('MentionSourceName').count().orderBy(desc('count')).limit(1000)


def get_sources_number(df_mentions):
    """
    :return: number of sources
    :type df_mentions: DataFrame
    :rtype: Column
    """
    return df_mentions.groupBy('MentionSourceName').count().count()


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


def get_media_cov(df_mentions, df_events):
    """
    :return: mentions number and Goldstein score for each event
    :type df_mentions: DataFrame
    :type df_events: DataFrame
    :rtype: DataFrame
    """
    # Computing the mediatic coverage of each event in the mentions database
    goldstein = df_events.select('GLOBALEVENTID', 'GoldsteinScale')
    ret = df_mentions.groupby('GLOBALEVENTID').count()
    return ret.join(goldstein, 'GLOBALEVENTID')




########
# Time #
########


# WORLDWIDE
def get_events_worldwide(events_df):
    """
    :return: events number for each month
    :type events_df: DataFrame
    :rtype: DataFrame
    """
    events_worldwide = events_df.groupBy('MonthYear_Date').count()
    return events_worldwide


# WORLDWIDE
udf_mention1 = UserDefinedFunction(lambda x: x.strftime('%Y%m'))
udf_mention2 = UserDefinedFunction(lambda x: datetime.strptime(x, '%Y%m').strftime('%m-%Y'))


def get_media_coverage_worldwide(df_mentions):
    """
    :return: mentions number for each month
    :type df_mentions: DataFrame
    :rtype: DataFrame
    """
    df = df_mentions.select('GLOBALEVENTID', 'MentionTimeDate')
    ret = df.withColumn('MentionTimeDate', udf_mention1(df.MentionTimeDate))
    return ret.groupBy('MentionTimeDate').count()


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


def get_Goldstein(df_events):
    """
    :return: average Goldstein ration per month
    :type df_events: DataFrame
    :rtype: DataFrame
    """
    df = df_events.select('MonthYear_Date', 'GoldsteinScale')
    return df.groupBy('MonthYear_Date').mean('GoldsteinScale')


def get_violentevents(df_events):
    """
    :return: violent events
    :type df_events: DataFrame
    :rtype: DataFrame
    """
    return df_events.filter(df_events.EventRootCode > 17)


def get_peacefullevents(df_events):
    """
    :return: peacefull events
    :type df_events: DataFrame
    :rtype: DataFrame
    """
    return df_events.filter(df_events.EventRootCode < 4)


def get_activity_byTypeCountry(df_events):
    violent = get_violentevents(df_events)
    peace = get_peacefullevents(df_events)
    df = peace.union(violent)

    total_event = df.count()
    get_events_percent = UserDefinedFunction(lambda x: x / total_event, DoubleType())

    count_type = df.groupby('ActionGeo_CountryCode', 'EventRootCode').agg(
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
    events_df.write.mode('overwrite').parquet("events_df.parquet")
    events_df = spark.read.parquet("events_df.parquet")
    print("events_df to parquet done")
    ret = events_df.groupBy('MonthYear_Date', 'ActionGeo_CountryCode').count()
    return ret


# returns the number of mentions for each country for each of the biggest sources (from 4 different countries)
def mentions_biggest_sources(df_mentions, df_events, selected_sources):
    mentions_selected_sources = df_mentions.filter(col('MentionSourceName').isin(selected_sources))
    mentions_selected_sources = mentions_selected_sources.join(df_events, 'GLOBALEVENTID')
    return mentions_selected_sources.groupBy('MentionSourceName', 'ActionGeo_CountryCode').agg(
        count('GLOBALEVENTID').alias('Number_Mentions')).orderBy('MentionSourceName', 'ActionGeo_CountryCode')


def get_media_cov_per_country(df_events, df_mentions):
    """
    :return: media coverage for each country for each month
    :type df_events: DataFrame
    :type df_mentions: DataFrame
    :rtype: DataFrame
    """
    df = df_mentions.select('GLOBALEVENTID', 'MentionTimeDate').join(
        df_events.select('GLOBALEVENTID', 'ActionGeo_CountryCode'), 'GLOBALEVENTID')
    df.write.mode('overwrite').parquet("df.parquet")
    df = spark.read.parquet("df.parquet")
    ret = df.withColumn('MentionTimeDate', udf_mention1(df.MentionTimeDate))
    ret = ret.groupBy('ActionGeo_CountryCode', 'MentionTimeDate').count()
    return ret


def get_activity_byTypeCountry_time(df_events):
    """
    :return: events per month per type per country
    :type df_events: DataFrame
    :rtype: DataFrame
    """
    df = df_events.select('GLOBALEVENTID', 'MonthYear_Date', 'EventRootCode', 'ActionGeo_CountryCode')
    df.write.mode('overwrite').parquet("df.parquet")
    df = spark.read.parquet("df.parquet")
    violent = get_violentevents(df)
    peace = get_peacefullevents(df)
    ret = peace.union(violent)
    return ret.groupby('MonthYear_Date', 'EventRootCode', 'ActionGeo_CountryCode').count()


def mentions_biggest_sources1(df_mentions, df_events, selected_sources):
    mentions_selected_sources = df_mentions.filter(col('MentionSourceName').isin(selected_sources))
    mentions_selected_sources = mentions_selected_sources.groupBy('MentionSourceName', 'GLOBALEVENTID').agg(
        count('GLOBALEVENTID').alias('Number_mentions_event'))
    mentions_selected_sources = mentions_selected_sources.join(df_events, 'GLOBALEVENTID').select('MentionSourceName',
                                                                                                  'GLOBALEVENTID',
                                                                                                  'Number_mentions_event',
                                                                                                  'ActionGeo_CountryCode')
    return mentions_selected_sources.groupBy('MentionSourceName', 'ActionGeo_CountryCode').agg(
        sum('Number_mentions_event').alias('Number_Mentions'))


def mentions_biggest_sources2(df_mentions, df_events, selected_sources):
    mentions_selected_sources = df_mentions.filter(col('MentionSourceName').isin(selected_sources))
    # for each sources finds the IDs of the events it mentiones
    mentions_selected_sources = mentions_selected_sources.groupBy('MentionSourceName', 'GLOBALEVENTID').count().select(
        'MentionSourceName', 'GLOBALEVENTID')
    # find the country for each of these events
    mentions_selected_sources = mentions_selected_sources.join(df_events, 'GLOBALEVENTID').select('MentionSourceName',
                                                                                                  'GLOBALEVENTID',
                                                                                                  'ActionGeo_CountryCode')
    # finds the overall number of events for each country in the 2 years
    events_country = df_events.groupBy('ActionGeo_CountryCode').agg(
        count('GLOBALEVENTID').alias('Number_events_country'))
    # for each country mentioned in the sources, associates its number of events in the 2 years
    sources_events = mentions_selected_sources.join(events_country, 'ActionGeo_CountryCode').select('MentionSourceName',
                                                                                                    'GLOBALEVENTID',
                                                                                                    'ActionGeo_CountryCode',
                                                                                                    'Number_events_country')
    # finds the number of events in each country mentioned by theses specific media sources
    return sources_events.groupBy('MentionSourceName', 'ActionGeo_CountryCode', 'Number_events_country').agg(
        count('GLOBALEVENTID').alias('Number_events_source'))


def get_av_media_coverage_country_type(df_mentions, df_events):
    violent = get_violentevents(df_events)
    peace = get_peacefullevents(df_events)
    df = peace.union(violent)
    df1 = df.join(df_mentions, 'GLOBALEVENTID').select('GLOBALEVENTID', 'ActionGeo_CountryCode', 'EventRootCode')
    df2 = df1.groupBy('ActionGeo_CountryCode', 'EventRootCode', 'GLOBALEVENTID').agg(
        count('GLOBALEVENTID').alias('Number_mentions'))
    return df2.groupBy('ActionGeo_CountryCode', 'EventRootCode').agg(
        mean('Number_mentions').alias('Average media coverage per event'))


def num_violent_pacif(df_events, df_mentions):
    """
    :return: mentions per event per country per eventrootcode
    :type df_events: DataFrame
    :type df_mentions: DataFrame
    :rtype: DataFrame
    """
    return df_events.join(df_mentions, 'GLOBALEVENTID').groupBy('GLOBALEVENTID', 'ActionGeo_CountryCode', 'EventRootCode').count()


def men_per_src_per_country(df_events, df_mentions):
    """
    :return: number of times each country is mentioned by each media source
    :type df_events: DataFrame
    :type df_mentions: DataFrame
    :rtype: DataFrame
    """
    return df_events.join(df_mentions, 'GLOBALEVENTID').groupBy('ActionGeo_CountryCode', 'MentionSourceName').count()

def get_events_media_attention():
    df = mentions.select('GLOBALEVENTID', 'EventTimeDate', 'MentionTimeDate')
    df.write.mode('overwrite').parquet("df.parquet")
    df = spark.read.parquet("df.parquet")
    print("df to parquet done")
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
