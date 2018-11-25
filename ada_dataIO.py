from ada_const import *
from ada_context import *
from ada_imports import *
import ada_const


def loadGDELT(dataset=EVENTS):
    global current_schema
    if dataset == EVENTS:
        current_schema = EVENTS_SCHEMA
    elif dataset == MENTIONS:
        current_schema = MENTIONS_SCHEMA
    else:
        return None
    return spark.read.csv(os.path.join(DATA_DIR, "*." + dataset + ".CSV"), sep="\t", schema=current_schema)


def saveDataFrame(df):
    if df is not None:
        df.coalesce(1).write.mode('overwrite').csv('df_save')
        return 0
    return 1
