from pyspark.sql.types import StructField, StructType, LongType, StringType, BooleanType, TimestampType

from shared.utilities import ClassificationColumnNames, InputColumnNames


def classification_lead_schema():
    return StructType(
        [StructField(InputColumnNames.LEAD_ID, StringType(), False),
         StructField(ClassificationColumnNames.SET_KEY, LongType(), False),
         StructField(ClassificationColumnNames.CLASSIF_TIMESTAMP, StringType(), False),
         StructField(ClassificationColumnNames.INSERTED_TIMESTAMP, StringType(), False)])


def classification_set_elem_xref_schema():
    return StructType(
        [StructField(ClassificationColumnNames.SET_KEY, LongType(), False),
         StructField(ClassificationColumnNames.ELEMENT_KEY, LongType(), False),
         StructField(ClassificationColumnNames.SUBCATEGORY_KEY, LongType(), False),
         StructField(ClassificationColumnNames.CATEGORY_KEY, LongType(), False),
         StructField(ClassificationColumnNames.INSERTED_TIMESTAMP, StringType(), False)])


def classification_subcategory_schema():
    return StructType(
        [StructField(ClassificationColumnNames.SUBCATEGORY_KEY, LongType(), False),
         StructField(ClassificationColumnNames.CATEGORY_KEY, LongType(), False),
         StructField(ClassificationColumnNames.SUBCATEGORY_NAME, StringType(), False),
         StructField(ClassificationColumnNames.DISPLAY_NAME, StringType(), False),
         StructField(ClassificationColumnNames.INSERTED_TIMESTAMP, TimestampType(), False)])


def expected_input_schema():
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType()),
         StructField(InputColumnNames.INPUT_ID, StringType()),
         StructField(InputColumnNames.INPUT_ID_TYPE, StringType()),
         StructField(InputColumnNames.AS_OF_TIME, StringType()),
         StructField(InputColumnNames.HAS_ERROR, BooleanType()),
         StructField(InputColumnNames.ERROR_MESSAGE, StringType())])


def expected_input_lead_transformed_schema():
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType()),
         StructField(InputColumnNames.AS_OF_TIME, StringType()),
         StructField(InputColumnNames.HAS_ERROR, BooleanType()),
         StructField(InputColumnNames.ERROR_MESSAGE, StringType()),
         StructField(InputColumnNames.INPUT_ID, StringType()),
         StructField(ClassificationColumnNames.SET_KEY, LongType())])


def expected_classification_result_schema():
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType()),
         StructField(InputColumnNames.INPUT_ID, StringType()),
         StructField(ClassificationColumnNames.SUBCATEGORY_KEY, StringType())
         ])
