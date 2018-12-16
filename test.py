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

    # Confidence in our data and 2month delay
    mentions = restric_cov(get_delay(mentions), 60)
    mentions = get_goodConfidence(mentions)
    mentions.write.mode('overwrite').parquet("mentions.parquet")
    mentions = spark.read.parquet("mentions.parquet")
    print("mentions to parquet done")
    events.write.mode('overwrite').parquet("events.parquet")
    events = spark.read.parquet("events.parquet")
    print("events to parquet done")

    saveDataFrame(more(mentions, events), 'get_activity_byGoldstein')

    print(mentions.count())
    print("<3")


def more(m, e):
    return get_media_cov(m, e).groupBy('GoldsteinScale').sum('count')


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


def Goldstein_mediaCov(df_mentions, df_events):
    media_coverage = get_media_cov(df_mentions, df_events)
    df = media_coverage.join(df_events, 'GLOBALEVENTID').select(df_events.GoldsteinScale, 'count')
    return df.groupBy('GoldsteinScale').agg(avg('count').alias('Average media coverage per event'))


def get_activity_byGoldstein(df_events):
    total_event = df_events.count()
    get_events_percent = UserDefinedFunction(lambda x: x / total_event, DoubleType())

    goldstein = df_events.groupby('GoldsteinScale').agg(count('GLOBALEVENTID').alias('Number Events')).orderBy(
        'GoldsteinScale')

    return goldstein.select(
        [get_events_percent(column).alias('Fraction of Events') if column == 'Number Events' else column for column in
         goldstein.columns])


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


def mentions_biggest_sources1(df_mentions, df_events, selected_sources):
    mentions_selected_sources = df_mentions.filter(col('MentionSourceName').isin(selected_sources))
    mentions_selected_sources = mentions_selected_sources.groupBy('MentionSourceName', 'GLOBALEVENTID').agg(count('GLOBALEVENTID').alias('Number_mentions_event'))
    mentions_selected_sources = mentions_selected_sources.join(df_events, 'GLOBALEVENTID').select('MentionSourceName',
                                                                                                  'GLOBALEVENTID',
                                                                                                  'Number_mentions_event',
                                                                                                  'ActionGeo_CountryCode')
    return mentions_selected_sources.groupBy('MentionSourceName', 'ActionGeo_CountryCode').agg(
        sum('Number_mentions_event').alias('Number_Mentions'))


def get_delay(df_mentions):
    # Get delay between event time and mention time

    timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    timeDiff = (unix_timestamp('MentionTimeDate', format=timeFmt) - unix_timestamp('EventTimeDate', format=timeFmt))

    return df_mentions.withColumn("Mention_delay", timeDiff)


def get_goodConfidence(df_mentions):
    index = df_mentions['Confidence'] > 20
    return df_mentions[df_mentions.schema.names][index]


def restric_cov(df_mentions, days_threshold):
    # Narrow down mentions to 2 month posterior to event mentions

    restric_index = df_mentions['Mention_Delay'] <= days_threshold * 24 * 3600

    return df_mentions[df_mentions.schema.names][restric_index]


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
