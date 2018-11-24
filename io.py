from const import *
from context import *
from imports import *


def loadGDELT(dataset=EVENTS):
    global current_schema
    if dataset == EVENTS:
        current_schema = const.EVENTS_SCHEMA
    elif dataset == MENTIONS:
        current_schema = const.MENTIONS_SCHEMA
    else:
        return null
    return spark.read.csv(os.path.join(DATA_DIR, "*." + dataset + ".CSV"), sep="\t", schema=current_schema)


def saveDataFrame(df):
    if df is not null:
        df.repartition(1).write.mode('overwrite').csv('df_save')
        return 0
    return 1
