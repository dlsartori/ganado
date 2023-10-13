from krnl_cfg import *
# from krnl_abstract_class_activity import *
from json import dumps, loads
from threading import Lock, Event

ACTIVITY_LOWER_LIMIT = 15
ACTIVITY_UPPER_LIMIT = 30
ACTIVITY_DAYS_TO_ALERT = 15
ACTIVITY_DAYS_TO_EXPIRE = 365
ACTIVITY_DEFAULT_OPERATOR = None
uidCh = '__'  # Used in Signature and Notifications to create unique field names. Chars MUST be ok with use in DB Names.
oprCh = '__opr'     # Particle added to "fldName" fields in DataTables to store operators strings belonging to "fldName"


GENERIC_OBJECT_ID_LIST = [1, 400]    # TODO: TEMPORAL ARREGLAR. Llevarlo a una tabla en Activities seteada correctamente


def moduleName():
    return str(os.path.basename(__file__))

