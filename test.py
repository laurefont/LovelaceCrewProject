from ada_const import *
from ada_context import *
from ada_imports import *
from ada_dataIO import *
from ada_dataCleaner import *

mentions = None


def main():
    """
    Computes and saves all dataframes needed for our statistics
    :return: 0 if successful
    """
    global mentions

    mentions = loadGDELT(MENTIONS)

    mentions = cleanMentions(mentions)

    # Confidence in our data and 2month delay
    mentions = restric_cov(get_delay(mentions), 60)
    mentions = get_goodConfidence(mentions)
    saveDataFrame(get_events_media_attention(), 'get_events_media_attention')
    print("<3")


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
