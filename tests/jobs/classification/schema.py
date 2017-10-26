from pyspark.sql.types import StructField, StructType, LongType, StringType, BooleanType, IntegerType

from shared.utilities import ClassificationLead, ClassificationSetElementXref, ClassificationSubcategory, \
    InputColumnNames


def classification_lead_schema():
    return StructType(
        [StructField(ClassificationLead.TOKEN, StringType()),
         StructField(ClassificationLead.CLASSIF_SET_KEY, StringType())])


def classification_set_elem_xref_schema():
    return StructType(
        [StructField(ClassificationSetElementXref.CLASSIF_SET_KEY, StringType()),
         StructField(ClassificationSetElementXref.CLASSIF_ELEMENT_KEY, LongType()),
         StructField(ClassificationSetElementXref.ELEMENT_CD, StringType()),
         StructField(ClassificationSetElementXref.ELEMENT_DISPLAY_NM, StringType()),
         StructField(ClassificationSetElementXref.CLASSIF_SUBCATEGORY_KEY, LongType()),
         StructField(ClassificationSetElementXref.SUBCATEGORY_CD, StringType()),
         StructField(ClassificationSetElementXref.SUBCATEGORY_DISPLAY_NM, StringType()),
         StructField(ClassificationSetElementXref.CLASSIF_CATEGORY_KEY, LongType()),
         StructField(ClassificationSetElementXref.CATEGORY_CD, StringType()),
         StructField(ClassificationSetElementXref.CATEGORY_DISPL_NM, StringType()),
         StructField(ClassificationSetElementXref.INSERT_TS, StringType()),
         StructField(ClassificationSetElementXref.CLASSIF_OWNER_NM, StringType()),
         StructField(ClassificationSetElementXref.INSERT_JOB_RUN_ID, IntegerType()),
         StructField(ClassificationSetElementXref.INSERT_BATCH_RUN_ID, IntegerType()),
         StructField(ClassificationSetElementXref.LOAD_ACTION_IND, StringType())
         ])


def classification_subcategory_schema():
    return StructType(
        [StructField(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY, LongType()),
         StructField(ClassificationSubcategory.CLASSIF_CATEGORY_KEY, LongType()),
         StructField(ClassificationSubcategory.SUBCATEGORY_CD, StringType()),
         StructField(ClassificationSubcategory.SUBCATEGORY_DISPLAY_NM, StringType()),
         StructField(ClassificationSubcategory.CLASSIF_OWNER_NM, StringType()),
         StructField(ClassificationSubcategory.INSERT_TS, StringType()),
         StructField(ClassificationSubcategory.INSERT_JOB_RUN_ID, IntegerType()),
         StructField(ClassificationSubcategory.INSERT_BATCH_RUN_ID, IntegerType()),
         StructField(ClassificationSubcategory.LOAD_ACTION_IND, StringType()),
         ])


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
         StructField(ClassificationLead.CLASSIF_SET_KEY, StringType())])


def expected_classification_result_schema():
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType()),
         StructField(InputColumnNames.INPUT_ID, StringType()),
         StructField(ClassificationSetElementXref.CLASSIF_SUBCATEGORY_KEY, StringType())
         ])
