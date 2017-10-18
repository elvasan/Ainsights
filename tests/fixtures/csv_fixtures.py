# from pyspark.sql.types import StructField, StructType, ArrayType, StringType, LongType


# def input_csv_schema():
#     return StructType([StructField("RecordId", LongType(), True), StructField("Leadids", StringType(), True),
#                        StructField("Phones", StringType(), True), StructField("Emails", StringType(), True),
#                        StructField("Devices", StringType(), True)])
#
#
# def input_transformed_schema():
#     return StructType([StructField("RecordId", LongType(), True), StructField("Leadids", StringType(), True),
#                        StructField("input_type", StringType(), False),
#                        StructField("phone_array", ArrayType(StringType()), True),
#                        StructField("email_array", ArrayType(StringType()), True),
#                        StructField("device_array", ArrayType(StringType()), True)])
