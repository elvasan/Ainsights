from pyspark.sql.types import StructField, StructType, LongType, StringType

from shared.utilities import PiiHashingColumnNames, InputColumnNames, ConsumerViewSchema


def expected_pii_hashing_schema():
    return StructType(
        [StructField(PiiHashingColumnNames.RECORD_ID, LongType()),
         StructField(PiiHashingColumnNames.INPUT_ID_RAW, StringType()),
         StructField(PiiHashingColumnNames.INPUT_ID, StringType()),
         StructField(PiiHashingColumnNames.INPUT_ID_TYPE, StringType())])


def expected_pii_hashing_consumer_view_transformed_schema():
    return StructType(
        [StructField(PiiHashingColumnNames.RECORD_ID, LongType()),
         StructField(ConsumerViewSchema.CLUSTER_ID, LongType())])


def expected_consumer_insights_result_schema():
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType()),
         StructField(InputColumnNames.INPUT_ID, StringType())])


def consumer_view_schema():
    return StructType(
        [StructField(ConsumerViewSchema.NODE_TYPE_CD, StringType()),
         StructField(ConsumerViewSchema.VALUE, StringType()),
         StructField(ConsumerViewSchema.CLUSTER_ID, LongType())])