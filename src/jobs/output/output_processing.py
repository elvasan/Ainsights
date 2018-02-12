from pyspark.sql.functions import col, concat, lit, asc

from shared.constants import GenericColumnNames, Environments, ClassificationSubcategory, InputColumnNames, \
    ThresholdValues, JoinTypes


def transform_scoring_columns_for_output(classify_subcategory_df, scored_results_df):
    """
    Get column rows as dictionary ex: { 'auto_sales': 'Auto Sales', 'education': 'Education' } and
    transforms all columns using dictionary (if not there just existing value)
    :param classify_subcategory_df: The subcategory table as a DataFrame
    :param scored_results_df: A DataFrame consisting of records that have been scored by category but whose headers
    need to be updated to a human readable format.
    :return: A final DataFrame ready to be written
    """
    class_dict = get_classifications_as_dictionary(classify_subcategory_df)
    return scored_results_df.select([col(c).alias(class_dict.get(c, c)) for c in scored_results_df.columns])


def get_classifications_as_dictionary(classify_subcategory_df):
    """
    Pulls out arrays of [ (subcategory, display), (subcategory, display) ]
    :param classify_subcategory_df: The subcategory table as a DataFrame
    :return: A Dict containing subcategory name and display name
    """
    row_list = [[i.subcategory_cd, i.subcategory_display_nm] for i in
                classify_subcategory_df.select(ClassificationSubcategory.SUBCATEGORY_CD,
                                               ClassificationSubcategory.SUBCATEGORY_DISPLAY_NM).collect()]
    class_dict = dict(row_list)
    # add in record_id mapping
    class_dict[InputColumnNames.RECORD_ID] = GenericColumnNames.RECORD_ID
    return class_dict


def build_output_csv_folder_name(environment, client_name, job_run_id):
    """
    Builds a path for writing the output of the program.
    Folder name should contain environment, aws_region, client_name
    Dev example: s3://jornaya-dev-us-east-1-aida-insights/beestest/output/beestest_aidainsights_201710241320
    Local example: ../samples/beestest/output/beestest_aidainsights_201710241320
    :param environment: The current execution environment
    :param client_name: The name of the client for which aida insights is running
    :param job_run_id: The id of the job run
    :return: A string containing the output path
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-aida-insights/'.format(environment, Environments.AWS_REGION)
    return '{0}app_data/{1}/{1}_{2}/output/'.format(bucket_prefix,
                                                    client_name,
                                                    job_run_id)


def append_output_location(output_path, location):
    """
    Simple helper function to return an output path for a specific location (input_summary, output_summary)
    by combining the output path prefix and the location postfix.

    :param output_path: A string value representing an output path, either local or s3
    :param location: A string to be appended to the output path.
    :return: A string representing the full output path
    """
    return output_path + location


def write_output(output_df, output_path, location, write_header="True"):
    """
    Builds the output location and writes to CSV format.
    :param output_df: The DataFrame being written
    :param output_path: The initial output location, either local or in s3.
    :param location: One of internal or external
    :param write_header: Whether to write DataFrame header to CSV, default is 'True'
    :return:
    """
    output_path = append_output_location(output_path, location)
    output_df \
        .coalesce(1) \
        .write \
        .csv(path=output_path, mode="overwrite", header=write_header)


def summarize_output_df(spark, external_output_df):
    order_col_name = "Order"
    categories = [[1, ThresholdValues.NOT_SEEN],
                  [2, ThresholdValues.EARLY_JOURNEY],
                  [3, ThresholdValues.LATE_JOURNEY]]

    res = spark.createDataFrame(categories, [order_col_name, GenericColumnNames.STAGE])

    output_count = external_output_df.count()
    for name in external_output_df.schema.names:
        if GenericColumnNames.RECORD_ID != name:
            sub_set = _create_sub_set(external_output_df, name, output_count)
            res = res.join(sub_set, GenericColumnNames.STAGE, JoinTypes.LEFT_JOIN)

    res = res.fillna("0 (0%)").sort(asc(order_col_name)).drop(order_col_name)

    col_number = len(res.schema.names)
    header = [""] * col_number
    body = [""] * col_number
    body[0] = GenericColumnNames.TOTAL_RECORDS
    body[1] = str(output_count)
    total = spark.createDataFrame([body, header, res.schema.names], res.schema.names)

    return total.unionAll(res)


def _create_sub_set(data, col_name, output_count):
    category_count = data.select(col(col_name)).groupBy(col_name).count()
    category_percentage = category_count.select(col_name,
                                                ((col("count") / output_count) * 100).cast('integer').alias('perc'))
    category_count_perc = category_count.join(category_percentage, col_name)
    res = category_count_perc.select(col_name, concat(col("count"), lit(" ("), col("perc"), lit("%)")).alias("count"))
    return res.select(col(col_name).alias(GenericColumnNames.STAGE), col("count").alias(col_name))
