from pyspark.sql.types import StructField, StructType, LongType, StringType, BooleanType

from shared.utilities import *


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
         StructField(COL_NAME_AS_OF_TIME, StringType()),
         StructField(COL_NAME_HAS_ERROR, StringType()),
         StructField(COL_NAME_ERROR_MESSAGE, BooleanType()),
         StructField(COL_NAME_INPUT_ID, StringType()),
         StructField(COL_NAME_CLASSIF_SUBCATEGORY_KEY, StringType())
         ])
