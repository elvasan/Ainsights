from pyspark.sql.functions import col

from shared.utilities import GenericColumnNames, Environments, ClassificationSubcategory, InputColumnNames, \
    OutputFileNames


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


def build_output_csv_folder_name(environment, client_name, time_stamp):
    """
    Builds a path for writing the output of the program.
    Folder name should contain environment, aws_region, client_name
    Dev example: s3://jornaya-dev-us-east-1-aida-insights/beestest/output/beestest_aidainsights_201710241320
    Local example: ../samples/beestest/output/beestest_aidainsights_201710241320
    :param environment: The current execution environment
    :param client_name: The name of the client for which aida insights is running
    :param time_stamp: The time the job was launched
    :return: A string containing the output path
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-aida-insights/'.format(environment, Environments.AWS_REGION)
    time_stamp_formatted = time_stamp.strftime(OutputFileNames.TIME_FORMAT)
    return '{0}{1}/output/{2}_{3}_{4}'.format(bucket_prefix,
                                              client_name,
                                              client_name,
                                              OutputFileNames.PRODUCT_NAME,
                                              time_stamp_formatted)


def write_output(output_path, classification_subcategory_df, scored_results_df):
    """
    Writes the output of AIDA Insights to a given location in CSV format.
    :param output_path: The output path used to write a csv
    :param classification_subcategory_df:
    :param scored_results_df:
    """
    results_df = transform_scoring_columns_for_output(classification_subcategory_df, scored_results_df)
    results_df \
        .coalesce(1) \
        .write \
        .csv(path=output_path, mode="overwrite", header="True")
