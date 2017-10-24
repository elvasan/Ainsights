from pyspark.sql.types import StructField, StructType, LongType, StringType, BooleanType, TimestampType

from shared.utilities import *


def classification_lead_schema():
    return StructType(
        [StructField(COL_NAME_LEAD_ID, StringType(), False),
         StructField(COL_NAME_CLASSIF_SET_KEY, LongType(), False),
         StructField(COL_NAME_CLASSIF_TIMESTAMP, StringType(), False),
         StructField(COL_NAME_INSERTED_TIMESTAMP, StringType(), False)])


def classification_set_elem_xref_schema():
    return StructType(
        [StructField(COL_NAME_CLASSIF_SET_KEY, LongType(), False),
         StructField(COL_NAME_CLASSIF_ELEMENT_KEY, LongType(), False),
         StructField(COL_NAME_CLASSIF_SUBCATEGORY_KEY, LongType(), False),
         StructField(COL_NAME_CLASSIF_CATEGORY_KEY, LongType(), False),
         StructField(COL_NAME_INSERTED_TIMESTAMP, StringType(), False)])


def classification_subcategory_schema():
    return StructType(
        [StructField(COL_NAME_CLASSIF_SUBCATEGORY_KEY, LongType(), False),
         StructField(COL_NAME_CLASSIF_CATEGORY_KEY, LongType(), False),
         StructField(COL_NAME_SUBCATEGORY_NAME, StringType(), False),
         StructField(COL_NAME_DISPLAY_NAME, StringType(), False),
         StructField(COL_NAME_INSERTED_TIMESTAMP, TimestampType(), False)])


def expected_input_schema():
    return StructType(
        [StructField(COL_NAME_RECORD_ID, LongType()),
         StructField(COL_NAME_INPUT_ID, StringType()),
         StructField(COL_NAME_INPUT_ID_TYPE, StringType()),
         StructField(COL_NAME_AS_OF_TIME, StringType()),
         StructField(COL_NAME_HAS_ERROR, BooleanType()),
         StructField(COL_NAME_ERROR_MESSAGE, StringType())])


def expected_input_lead_transformed_schema():
    return StructType(
        [StructField(COL_NAME_RECORD_ID, LongType()),
         StructField(COL_NAME_AS_OF_TIME, StringType()),
         StructField(COL_NAME_HAS_ERROR, BooleanType()),
         StructField(COL_NAME_ERROR_MESSAGE, StringType()),
         StructField(COL_NAME_INPUT_ID, StringType()),
         StructField(COL_NAME_CLASSIF_SET_KEY, LongType())])


def expected_classification_result_schema():
    return StructType(
        [StructField(COL_NAME_RECORD_ID, LongType()),
         StructField(COL_NAME_INPUT_ID, StringType()),
         StructField(COL_NAME_CLASSIF_SUBCATEGORY_KEY, StringType())
         ])
