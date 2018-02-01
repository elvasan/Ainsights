from pyspark.sql import functions as pyspark_functions
# TODO just a basic count here to test summary file s3 location - jk


def summarize_output_df(external_output_df):

    number_of_records_df = external_output_df.agg(pyspark_functions.count("*").alias("total_records"))

    return number_of_records_df


def summarize_input_df(input_file_df):

    number_of_records_df = input_file_df.agg(pyspark_functions.count("*").alias("total_records"))

    return number_of_records_df
