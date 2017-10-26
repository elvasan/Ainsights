from datetime import datetime

import pytest

from jobs.output.output_processing import get_classifications_as_dictionary, transform_scoring_columns_for_output, \
    build_output_csv_folder_name
from shared.utilities import GenericColumnNames, Environments, ClassificationCategoryDisplayNames, \
    ClassificationCategoryAbbreviations, InputColumnNames, OutputFileNames, ClassificationColumnNames

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
    extracted_row_values = extract_rows_for_col(result_df, expected_col_names, InputColumnNames.RECORD_ID)
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
    extracted_row_values = extract_rows_for_col(result_df, expected_col_names, InputColumnNames.RECORD_ID)
    assert extracted_row_values == raw_input_data


def test_build_output_csv_folder_name_dev():
    the_time = datetime.strptime("201710241230", OutputFileNames.TIME_FORMAT)
    full_name = build_output_csv_folder_name(Environments.DEV, "beestest", the_time)
    expected_name = 's3://jornaya-dev-us-east-1-aida-insights/beestest/output/beestest_aidainsights_201710241230'
    assert full_name == expected_name


def test_build_output_csv_folder_name_qa():
    the_time = datetime.strptime("201710310130", OutputFileNames.TIME_FORMAT)
    full_name = build_output_csv_folder_name(Environments.QA, "beestest", the_time)
    expected_name = 's3://jornaya-qa-us-east-1-aida-insights/beestest/output/beestest_aidainsights_201710310130'
    assert full_name == expected_name


def test_build_output_csv_folder_name_staging():
    the_time = datetime.strptime("201710310100", OutputFileNames.TIME_FORMAT)
    full_name = build_output_csv_folder_name(Environments.STAGING, "beestest", the_time)
    expected_name = 's3://jornaya-staging-us-east-1-aida-insights/beestest/output/beestest_aidainsights_201710310100'
    assert full_name == expected_name


def test_build_output_csv_folder_name_prod():
    the_time = datetime.strptime("201710241230", OutputFileNames.TIME_FORMAT)
    full_name = build_output_csv_folder_name(Environments.PROD, "beestest", the_time)
    expected_name = 's3://jornaya-prod-us-east-1-aida-insights/beestest/output/beestest_aidainsights_201710241230'
    assert full_name == expected_name


def test_build_output_csv_folder_name_local():
    the_time = datetime.strptime("201710241230", OutputFileNames.TIME_FORMAT)
    full_name = build_output_csv_folder_name(Environments.LOCAL, "beestest", the_time)
    expected_name = '../samples/beestest/output/beestest_aidainsights_201710241230'
    assert full_name == expected_name


def define_classification_subcategory_df(spark_session):
    raw_hash_rows = [(1, 1, ClassificationCategoryAbbreviations.AUTO_SALES,
                      ClassificationCategoryDisplayNames.AUTO_SALES, 0),
                     (2, 1, ClassificationCategoryAbbreviations.EDUCATION,
                      ClassificationCategoryDisplayNames.EDUCATION, 0),
                     (9, 1, ClassificationCategoryAbbreviations.OTHER,
                      ClassificationCategoryDisplayNames.OTHER, 0)]
    col_names = [ClassificationColumnNames.SUBCATEGORY_KEY, ClassificationColumnNames.CATEGORY_KEY,
                 ClassificationColumnNames.SUBCATEGORY_NAME, ClassificationColumnNames.DISPLAY_NAME,
                 ClassificationColumnNames.INSERTED_TIMESTAMP]
    return spark_session.createDataFrame(raw_hash_rows, col_names)


# TODO: merge into other shared utils
def extract_rows_for_col(data_frame, col_names, order_by_column):
    # list comprehension is only way I can think of to make this easy
    # get Row objects and translate to Dict type
    rows_as_dicts = [i.asDict() for i in data_frame.select(col_names).orderBy(order_by_column).collect()]

    # from Dict get values in same order as column name COLUMN order
    list_values = [[row_dict.get(col_name) for col_name in col_names] for row_dict in rows_as_dicts]
    return list_values
