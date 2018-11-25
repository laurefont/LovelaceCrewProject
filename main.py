from ada_const import *
from ada_context import *
from ada_imports import *
from ada_dataIO import *
from ada_dataCleaner import *


def main():
    """
    Simple stat just to be sure we can run it on the cluster

    computes the number of mentions per country across the whole dataset.
    :return: 0 if successful
    """
    events = loadGDELT(EVENTS)
    mentions = loadGDELT(MENTIONS)

    events = cleanEvents(events)
    mentions = cleanMentions(mentions)

    events.registerTempTable('events')
    mentions.registerTempTable('mentions')

    loc_events = events.dropna(subset='ActionGeo_CountryCode')
    mentions_count = mentions.groupBy('GLOBALEVENTID').count()
    country_count = mentions_count.join(loc_events, 'GLOBALEVENTID').groupBy('ActionGeo_CountryCode').sum(
        'count')

    saveDataFrame(country_count)
    return 0


if __name__ == "__main__":
    main()
