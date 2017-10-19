from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

from shared.utilities import *


# TODO: Solidify Data Types
def classification_lead_schema():
    return StructType([StructField(COL_NAME_LEAD_ID, StringType(), False),
                       StructField(COL_NAME_CLASSIF_SET_KEY, LongType(), False),
                       StructField(COL_NAME_CLASSIF_TIMESTAMP, StringType(), False),
                       StructField(COL_NAME_INSERTED_TIMESTAMP, StringType(), False)])


def classification_set_elem_xref_schema():
    return StructType([StructField(COL_NAME_CLASSIF_SET_KEY, LongType(), False),
                       StructField(COL_NAME_CLASSIF_ELEMENT_KEY, LongType(), False),
                       StructField(COL_NAME_CLASSIF_SUBCATEGORY_KEY, LongType(), False),
                       StructField(COL_NAME_CLASSIF_CATEGORY_KEY, LongType(), False),
                       StructField(COL_NAME_INSERTED_TIMESTAMP, StringType(), False)])


def classification_subcategory_schema():
    return StructType([StructField(COL_NAME_CLASSIF_SUBCATEGORY_KEY, LongType(), False),
                       StructField(COL_NAME_CLASSIF_CATEGORY_KEY, LongType(), False),
                       StructField(COL_NAME_SUBCATEGORY_NAME, StringType(), False),
                       StructField(COL_NAME_DISPLAY_NAME, StringType(), False),
                       StructField(COL_NAME_INSERTED_TIMESTAMP, TimestampType(), False)])
