import pytest

from jobs.classification import classify
from shared.utilities import Environments, ClassificationSetElementXref
from tests.jobs.classification.schema import expected_input_schema, \
    expected_classification_result_schema, expected_input_lead_transformed_schema, classification_lead_schema, \
    classification_set_elem_xref_schema

spark_session_enabled = pytest.mark.usefixtures("spark_session")


def extract_rows_for_col(data_frame, col_name):
    return [i[col_name] for i in data_frame.select(col_name).collect()]


def test_get_classification_schema_location_returns_correct_for_dev():
    file_name = classify.get_classification_schema_location(Environments.DEV, 'classif_lead')
    assert 's3://jornaya-dev-us-east-1-prj/classification/classif_lead/' == file_name


def test_get_classification_schema_location_returns_correct_for_qa():
    file_name = classify.get_classification_schema_location(Environments.QA, 'classif_lead')
    assert 's3://jornaya-qa-us-east-1-prj/classification/classif_lead/' == file_name


def test_get_classification_schema_location_returns_correct_for_staging():
    file_name = classify.get_classification_schema_location(Environments.STAGING, 'classif_lead')
    assert 's3://jornaya-staging-us-east-1-prj/classification/classif_lead/' == file_name


def test_get_classification_schema_location_returns_correct_for_prod():
    file_name = classify.get_classification_schema_location(Environments.PROD, 'classif_lead')
    assert 's3://jornaya-prod-us-east-1-prj/classification/classif_lead/' == file_name


def test_get_classification_schema_location_returns_correct_for_local():
    file_name = classify.get_classification_schema_location(Environments.LOCAL, 'classif_lead')
    assert '../samples/classification/classif_lead/' == file_name


def test_join_input_to_lead_data_frame_returns_expected_columns(spark_session):
    input_row = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 'leadid', '10/17/2017', False, '')]
    input_df = spark_session.createDataFrame(input_row, expected_input_schema())

    lead_row = [('LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 5)]
    lead_dataframe = spark_session.createDataFrame(lead_row, classification_lead_schema())

    result = classify.join_input_to_lead_df(input_df, lead_dataframe)

    assert sorted(expected_input_lead_transformed_schema().names) == sorted(result.schema.names)


def test_single_join_input_to_lead_df_with_no_matching_lead_id_returns_null_for_classification_key(spark_session):
    input_set = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 'leadid', '10/17/2017', False, '')]
    input_df = spark_session.createDataFrame(input_set, expected_input_schema())

    lead_row = [('LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', 5)]
    lead_dataframe = spark_session.createDataFrame(lead_row, classification_lead_schema())

    result = classify.join_input_to_lead_df(input_df, lead_dataframe)
    extracted_results = extract_rows_for_col(result, ClassificationSetElementXref.CLASSIF_SET_KEY)
    expected_col_results = [None]

    assert sorted(expected_input_lead_transformed_schema().names) == sorted(result.schema.names)
    assert expected_col_results == extracted_results


def test_multiple_join_input_to_lead_df_with_no_matching_lead_id_returns_null_for_classification_key(spark_session):
    input_set = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 'leadid', '10/17/2017', False, ''),
                 (2, 'LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', 'leadid', '10/17/2017', False, '')]
    input_df = spark_session.createDataFrame(input_set, expected_input_schema())

    lead_row = [('LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', '5')]
    lead_dataframe = spark_session.createDataFrame(lead_row, classification_lead_schema())

    result = classify.join_input_to_lead_df(input_df, lead_dataframe)
    extracted_results = extract_rows_for_col(result, ClassificationSetElementXref.CLASSIF_SET_KEY)
    expected_col_results = [None, '5']

    assert sorted(expected_input_lead_transformed_schema().names) == sorted(result.schema.names)
    assert expected_col_results == extracted_results


def test_join_input_to_classification_set_df_returns_expected_columns(spark_session):
    input_row = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 5)]
    input_df = spark_session.createDataFrame(input_row, expected_input_lead_transformed_schema())

    classification_row = [(5, 1)]
    classification_df = spark_session.createDataFrame(classification_row, classification_set_elem_xref_schema())

    result = classify.join_input_to_classification_set_df(input_df, classification_df)

    assert sorted(expected_classification_result_schema().names) == sorted(result.schema.names)


def test_join_input_to_classification_set_df_with_null_returns_expected_columns(spark_session):
    input_row = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 5),
                 (2, 'LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', None)]
    input_df = spark_session.createDataFrame(input_row, expected_input_lead_transformed_schema())

    classification_row = [(5, 1)]
    classification_df = spark_session.createDataFrame(classification_row, classification_set_elem_xref_schema())

    result = classify.join_input_to_classification_set_df(input_df, classification_df)
    extracted_results = extract_rows_for_col(result, ClassificationSetElementXref.CLASSIF_SUBCATEGORY_KEY)
    expected_col_results = [None, 1]

    assert sorted(expected_classification_result_schema().names) == sorted(result.schema.names)
    assert extracted_results == expected_col_results
