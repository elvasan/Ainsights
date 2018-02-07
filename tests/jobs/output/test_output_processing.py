import pytest

from jobs.output import output_processing as output
from shared.constants import GenericColumnNames, Environments, ClassificationCategoryDisplayNames, \
    ClassificationCategoryAbbreviations, InputColumnNames, ClassificationSubcategory, Test, OutputFileNames, \
    ThresholdValues
from tests.helpers import extract_rows_for_col_with_order

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_get_classifications_as_dictionary_returns_dictionary_with_recordid_added(spark_session):
    class_df = define_classification_subcategory_df(spark_session)
    result_dict = output.get_classifications_as_dictionary(class_df)
    expected_dict = {ClassificationCategoryAbbreviations.AUTO_SALES: ClassificationCategoryDisplayNames.AUTO_SALES,
                     ClassificationCategoryAbbreviations.EDUCATION: ClassificationCategoryDisplayNames.EDUCATION,
                     ClassificationCategoryAbbreviations.OTHER: ClassificationCategoryDisplayNames.OTHER,
                     InputColumnNames.RECORD_ID: GenericColumnNames.RECORD_ID}
    assert result_dict == expected_dict


def test_transform_output_scoring_columns(spark_session):
    raw_input_data = [
        [10, 1, 0, 0],
        [20, 0, 1, 0],
        [30, 0, 0, 1]
    ]
    col_names = [InputColumnNames.RECORD_ID, ClassificationCategoryAbbreviations.AUTO_SALES,
                 ClassificationCategoryAbbreviations.EDUCATION, ClassificationCategoryAbbreviations.OTHER]
    class_df = define_classification_subcategory_df(spark_session)
    input_df = spark_session.createDataFrame(raw_input_data, col_names)
    result_df = output.transform_scoring_columns_for_output(class_df, input_df)
    expected_col_names = [GenericColumnNames.RECORD_ID, ClassificationCategoryDisplayNames.AUTO_SALES,
                          ClassificationCategoryDisplayNames.EDUCATION, ClassificationCategoryDisplayNames.OTHER]
    assert sorted(result_df.schema.names) == sorted(expected_col_names)
    # pull rows from data frame using translated column names
    extracted_row_values = extract_rows_for_col_with_order(result_df, expected_col_names, InputColumnNames.RECORD_ID)
    assert extracted_row_values == raw_input_data


def test_transform_output_scoring_columns_with_non_translatable_column(spark_session):
    raw_input_data = [
        [10, 1, 0, 0, False],
        [20, 0, 1, 0, False],
        [30, 0, 0, 1, True]
    ]
    col_names = [InputColumnNames.RECORD_ID, ClassificationCategoryAbbreviations.AUTO_SALES,
                 ClassificationCategoryAbbreviations.EDUCATION, ClassificationCategoryAbbreviations.OTHER, 'has_error']
    class_df = define_classification_subcategory_df(spark_session)
    input_df = spark_session.createDataFrame(raw_input_data, col_names)
    result_df = output.transform_scoring_columns_for_output(class_df, input_df)
    expected_col_names = [GenericColumnNames.RECORD_ID, ClassificationCategoryDisplayNames.AUTO_SALES,
                          ClassificationCategoryDisplayNames.EDUCATION, ClassificationCategoryDisplayNames.OTHER,
                          "has_error"]
    assert sorted(result_df.schema.names) == sorted(expected_col_names)
    # pull rows from data frame using translated column names
    extracted_row_values = extract_rows_for_col_with_order(result_df, expected_col_names, InputColumnNames.RECORD_ID)
    assert extracted_row_values == raw_input_data


def test_build_internal_output_csv_folder_name_dev():
    full_name = output.build_output_csv_folder_name(Environments.DEV, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = 's3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_build_internal_output_csv_folder_name_qa():
    full_name = output.build_output_csv_folder_name(Environments.QA, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = 's3://jornaya-qa-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_build_internal_output_csv_folder_name_staging():
    full_name = output.build_output_csv_folder_name(Environments.STAGING, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = 's3://jornaya-staging-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_build_internal_output_csv_folder_name_prod():
    full_name = output.build_output_csv_folder_name(Environments.PROD, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = 's3://jornaya-prod-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_build_internal_output_csv_folder_name_local():
    full_name = output.build_output_csv_folder_name(Environments.LOCAL, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = '../samples/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_dev():
    full_name = output.build_output_csv_folder_name(Environments.DEV, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = 's3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_qa():
    full_name = output.build_output_csv_folder_name(Environments.QA, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = 's3://jornaya-qa-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_staging():
    full_name = output.build_output_csv_folder_name(Environments.STAGING, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = 's3://jornaya-staging-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_prod():
    full_name = output.build_output_csv_folder_name(Environments.PROD, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = 's3://jornaya-prod-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_local():
    full_name = output.build_output_csv_folder_name(Environments.LOCAL, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    expected_name = '../samples/app_data/beestest/beestest_2018_01_02/output/'
    assert full_name == expected_name


def test_append_output_location_returns_results_internal():
    output_path = "../samples/app_data/beestest/beestest_2018_01_02/output/"
    location = OutputFileNames.INTERNAL
    result = output.append_output_location(output_path, location)
    assert "../samples/app_data/beestest/beestest_2018_01_02/output/results_internal" == result


def test_append_output_location_returns_results_external():
    output_path = "s3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/output/"
    location = OutputFileNames.EXTERNAL
    result = output.append_output_location(output_path, location)
    assert "s3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/output/results_external" == result


def test_append_output_location_returns_input_summary():
    output_path = "../samples/app_data/beestest/beestest_2018_01_02/output/"
    location = OutputFileNames.INPUT_SUMMARY
    result = output.append_output_location(output_path, location)
    assert "../samples/app_data/beestest/beestest_2018_01_02/output/input_summary" == result


def test_append_output_location_returns_output_summary():
    output_path = "s3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/output/"
    location = OutputFileNames.OUTPUT_SUMMARY
    result = output.append_output_location(output_path, location)
    assert "s3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/output/output_summary" == result


def define_classification_subcategory_df(spark_session):
    raw_hash_rows = [(1,
                      1,
                      ClassificationCategoryAbbreviations.AUTO_SALES,
                      ClassificationCategoryDisplayNames.AUTO_SALES,
                      0),
                     (2,
                      1,
                      ClassificationCategoryAbbreviations.EDUCATION,
                      ClassificationCategoryDisplayNames.EDUCATION,
                      0),
                     (9,
                      1,
                      ClassificationCategoryAbbreviations.OTHER,
                      ClassificationCategoryDisplayNames.OTHER,
                      0)]

    col_names = [ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY,
                 ClassificationSubcategory.CLASSIF_CATEGORY_KEY,
                 ClassificationSubcategory.SUBCATEGORY_CD,
                 ClassificationSubcategory.SUBCATEGORY_DISPLAY_NM,
                 ClassificationSubcategory.INSERT_TS]
    return spark_session.createDataFrame(raw_hash_rows, col_names)


def test_summarize_output_df(spark_session):
    rows = [
        ("1", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("2", "NOT_SEEN", "NOT_SEEN", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("3", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("4", "NOT_SEEN", "NOT_SEEN", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("5", "NOT_SEEN", "EARLY_JOURNEY", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN", "NOT_SEEN"),
        ("6", "NOT_SEEN", "NOT_SEEN", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("7", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("8", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("9", "NOT_SEEN", "NOT_SEEN", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("10", "NOT_SEEN", "NOT_SEEN", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("11", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("12", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("13", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("14", "NOT_SEEN", "NOT_SEEN", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("15", "NOT_SEEN", "NOT_SEEN", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("16", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN"),
        ("17", "NOT_SEEN", "NOT_SEEN", "EARLY_JOURNEY", "EARLY_JOURNEY", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN", "NOT_SEEN",
         "NOT_SEEN")]

    schema = ["recordid", "Auto Sales", "Education", "Insurance", "Financial Services", "Real Estate", "Jobs", "Legal",
              "Home Services", "Other"]
    input_df = spark_session.createDataFrame(rows, schema)
    res = output.summarize_output_df(spark_session, input_df)
    plain_data = res.collect()
    assert res.count() == 6
    assert len(res.schema.names) == 10
    # Check total records count
    assert plain_data[0][1] == '17'

    # Check some data
    assert plain_data[3][3] == '8 (47%)'
    assert plain_data[4][3] == '9 (52%)'
    assert plain_data[5][3] == '0 (0%)'
    assert plain_data[3][5] == '17 (100%)'

    # Check Stage order
    assert plain_data[3][0] == ThresholdValues.NOT_SEEN
    assert plain_data[4][0] == ThresholdValues.EARLY_JOURNEY
    assert plain_data[5][0] == ThresholdValues.LATE_JOURNEY

    # Check all columns present
    assert plain_data[2][1] == 'Auto Sales'
    assert plain_data[2][2] == 'Education'
    assert plain_data[2][3] == 'Insurance'
    assert plain_data[2][4] == 'Financial Services'
    assert plain_data[2][5] == 'Real Estate'
    assert plain_data[2][6] == 'Jobs'
    assert plain_data[2][7] == 'Legal'
    assert plain_data[2][8] == 'Home Services'
    assert plain_data[2][9] == 'Other'
