from pyspark.sql.types import StructField, StructType, LongType, StringType, IntegerType

from shared.constants import ClassificationLead, ClassificationSetElementXref, ClassificationSubcategory, \
    InputColumnNames, ConfigurationSchema, LeadEventSchema


def classification_lead_schema():
    return StructType(
        [StructField(ClassificationLead.LEAD_ID, StringType()),
         StructField(ClassificationLead.CLASSIF_SET_KEY, StringType())])


def classification_set_elem_xref_schema():
    return StructType(
        [StructField(ClassificationSetElementXref.CLASSIF_SET_KEY, StringType()),
         StructField(ClassificationSetElementXref.CLASSIF_SUBCATEGORY_KEY, LongType())
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
         StructField(LeadEventSchema.CREATION_TS, StringType())])


def expected_input_lead_transformed_schema():
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType()),
         StructField(InputColumnNames.INPUT_ID, StringType()),
         StructField(ClassificationLead.CLASSIF_SET_KEY, StringType()),
         StructField(LeadEventSchema.CREATION_TS, StringType())])


def expected_classification_result_schema():
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType()),
         StructField(ClassificationSetElementXref.CLASSIF_SUBCATEGORY_KEY, StringType()),
         StructField(LeadEventSchema.CREATION_TS, StringType())])


def configuration_schema():
    return StructType(
        [StructField(ConfigurationSchema.OPTION, StringType()),
         StructField(ConfigurationSchema.CONFIG_ABBREV, StringType()),
         StructField(ConfigurationSchema.VALUE, StringType())])


def expected_transformed_configuration_schema():
    return StructType(
        [StructField(ConfigurationSchema.OPTION, StringType()),
         StructField(ConfigurationSchema.CONFIG_ABBREV, StringType()),
         StructField(ConfigurationSchema.VALUE, StringType()),
         StructField(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY, StringType())])


def expected_classif_lookback_result_schema():
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType()),
         StructField(ClassificationLead.CLASSIF_SUBCATEGORY_KEY, StringType())])
