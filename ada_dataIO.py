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


def saveDataFrame(df, name):
    if df is not None:
        df.write.mode('overwrite').csv(os.path.join(OUT_DIR, name), header='true')
        return 0
    return 1
