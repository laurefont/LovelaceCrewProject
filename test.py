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


if __name__ == "__main__":
    main()
