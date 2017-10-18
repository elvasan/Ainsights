# def load_dataframe_from_csv(spark_session, file_location):
#     return spark_session.read.format("csv") \
#         .option("header", "true") \
#         .option("inferSchema", "true") \
#         .load(file_location)
