from ada_const import *

"""
two possible values, CLUSTER or LOCAL
"""
current_context = LOCAL

try:
    import ada_dummy
except:
    current_context = CLUSTER
