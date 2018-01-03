import pytest

from jobs.output.output_processing import get_classifications_as_dictionary, transform_scoring_columns_for_output, \
    build_output_csv_folder_name
from shared.constants import GenericColumnNames, Environments, ClassificationCategoryDisplayNames, \
    ClassificationCategoryAbbreviations, InputColumnNames, OutputFileNames, ClassificationSubcategory
from shared.constants import Test
from tests.helpers import extract_rows_for_col_with_order

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_get_classifications_as_dictionary_returns_dictionary_with_recordid_added(spark_session):
    class_df = define_classification_subcategory_df(spark_session)
    result_dict = get_classifications_as_dictionary(class_df)
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
    result_df = transform_scoring_columns_for_output(class_df, input_df)
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
    result_df = transform_scoring_columns_for_output(class_df, input_df)
    expected_col_names = [GenericColumnNames.RECORD_ID, ClassificationCategoryDisplayNames.AUTO_SALES,
                          ClassificationCategoryDisplayNames.EDUCATION, ClassificationCategoryDisplayNames.OTHER,
                          "has_error"]
    assert sorted(result_df.schema.names) == sorted(expected_col_names)
    # pull rows from data frame using translated column names
    extracted_row_values = extract_rows_for_col_with_order(result_df, expected_col_names, InputColumnNames.RECORD_ID)
    assert extracted_row_values == raw_input_data


def test_build_internal_output_csv_folder_name_dev():
    full_name = build_output_csv_folder_name(Environments.DEV, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.INTERNAL)
    expected_name = 's3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/results_internal'
    assert full_name == expected_name


def test_build_internal_output_csv_folder_name_qa():
    full_name = build_output_csv_folder_name(Environments.QA, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.INTERNAL)
    expected_name = 's3://jornaya-qa-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/results_internal'
    assert full_name == expected_name


def test_build_internal_output_csv_folder_name_staging():
    full_name = build_output_csv_folder_name(Environments.STAGING, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.INTERNAL)
    expected_name = 's3://jornaya-staging-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/results_internal'
    assert full_name == expected_name


def test_build_internal_output_csv_folder_name_prod():
    full_name = build_output_csv_folder_name(Environments.PROD, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.INTERNAL)
    expected_name = 's3://jornaya-prod-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/results_internal'
    assert full_name == expected_name


def test_build_internal_output_csv_folder_name_local():
    full_name = build_output_csv_folder_name(Environments.LOCAL, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.INTERNAL)
    expected_name = '../samples/app_data/beestest/beestest_2018_01_02/output/results_internal'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_dev():
    full_name = build_output_csv_folder_name(Environments.DEV, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.EXTERNAL)
    expected_name = 's3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/results_external'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_qa():
    full_name = build_output_csv_folder_name(Environments.QA, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.EXTERNAL)
    expected_name = 's3://jornaya-qa-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/results_external'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_staging():
    full_name = build_output_csv_folder_name(Environments.STAGING, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.EXTERNAL)
    expected_name = 's3://jornaya-staging-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/results_external'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_prod():
    full_name = build_output_csv_folder_name(Environments.PROD, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.EXTERNAL)
    expected_name = 's3://jornaya-prod-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/output/results_external'
    assert full_name == expected_name


def test_build_external_output_csv_folder_name_local():
    full_name = build_output_csv_folder_name(Environments.LOCAL, Test.CLIENT_NAME, Test.JOB_RUN_ID,
                                             OutputFileNames.EXTERNAL)
    expected_name = '../samples/app_data/beestest/beestest_2018_01_02/output/results_external'
    assert full_name == expected_name


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
