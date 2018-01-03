from unittest import mock

import py4j
import pytest
from pyspark.sql.types import StructField, StructType, StringType, LongType, BooleanType
from pyspark.sql.utils import AnalysisException

from jobs.input.input_processing import transform_input_csv, build_input_csv_file_name, input_csv_schema, \
    load_csv_file, process_input_file
from shared.constants import Environments, InputColumnNames, IdentifierTypes, Test

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_build_input_csv_file_name_returns_correct_dev_env_name():
    full_name = build_input_csv_file_name(Environments.DEV, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == 's3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_build_input_csv_file_name_returns_correct_qa_env_name():
    full_name = build_input_csv_file_name(Environments.QA, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == 's3://jornaya-qa-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_build_input_csv_file_name_returns_correct_staging_env_name():
    full_name = build_input_csv_file_name(Environments.STAGING, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == 's3://jornaya-staging-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_build_input_csv_file_name_returns_correct_prod_env_name():
    full_name = build_input_csv_file_name(Environments.PROD, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == 's3://jornaya-prod-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_build_input_csv_file_name_returns_local_env_name_as_samples_directory():
    full_name = build_input_csv_file_name(Environments.LOCAL, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == '../samples/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_transform_input_csv_returns_empty_data_frame_when_no_data_present(spark_session):
    input_csv_data_frame = spark_session.createDataFrame([], input_csv_schema())
    result_data_frame = transform_input_csv(input_csv_data_frame)
    extracted_row_values = [[i.record_id, i.input_id_raw, i.input_id_type] for i in result_data_frame.select(
        result_data_frame.record_id, result_data_frame.input_id_raw, result_data_frame.input_id_type).collect()]
    assert extracted_row_values == []


def test_transform_input_csv_returns_proper_data_frame_schema_columns(spark_session):
    input_csv_data_frame = spark_session.createDataFrame([], input_csv_schema())
    result_data_frame = transform_input_csv(input_csv_data_frame)
    assert sorted(result_data_frame.schema.names) == sorted(input_csv_transformed_schema().names)


def test_load_csv_file_loads_file(spark_session, tmpdir):
    csv_file = tmpdir.mkdir('data').join('beestest_yyyy_mm_dd_hh_mm_ss_ffffff.csv')
    csv_text = "recordid,phone01,phone02,phone03,phone04,email01,email02,email03,leadid01,leadid02,leadid03,asof\r" \
               "1,,,,,,,,LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA,,,\r" \
               "2,,,,,,,,LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB,,,\r"
    csv_file.write(csv_text)
    loaded_df = load_csv_file(spark_session, csv_file.strpath, input_csv_schema())
    assert loaded_df.count() == 2


def test_process_input_file_end_to_end(spark_session, tmpdir, monkeypatch):
    csv_file = tmpdir.mkdir('data').join('beestest_yyyy_mm_dd_hh_mm_ss_ffffff.csv')
    csv_text = "recordid,phone01,phone02,phone03,phone04,email01,email02,email03,leadid01,leadid02,leadid03,asof\r" \
               "1,,,,,,,,LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA,,,\r" \
               "2,,,,,,,,LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB,,,\r"
    csv_file.write(csv_text)
    mock_logger = mock.Mock()

    # mock csv file name function to return local unit test csv file from tmp directory
    monkeypatch.setattr("jobs.input.input_processing.build_input_csv_file_name", lambda x, y, z: csv_file.strpath)
    result_df = process_input_file(spark_session, mock_logger, Test.CLIENT_NAME, "unit_test", Test.JOB_RUN_ID)
    extracted_row_values = [[i.record_id, i.input_id_raw, i.input_id_type, i.has_error, i.error_message] for i
                            in result_df.select(
            result_df.record_id, result_df.input_id_raw, result_df.input_id_type,
            result_df.has_error, result_df.error_message).collect()]
    expected_rows = [
        [1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', IdentifierTypes.LEADID, False, None],
        [2, 'LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', IdentifierTypes.LEADID, False, None]]
    assert extracted_row_values == expected_rows


def test_process_input_file_throws_analysis_exception_with_missing_input_file(spark_session, monkeypatch):
    mock_logger = mock.Mock()
    # mock csv file name function to return invalid path
    monkeypatch.setattr("jobs.input.input_processing.build_input_csv_file_name", lambda x, y, z: "invalid_file_name")
    with pytest.raises(AnalysisException):
        process_input_file(spark_session, mock_logger, Test.CLIENT_NAME, "unit_test", Test.JOB_RUN_ID)


def test_process_input_file_with_invalid_csv_file_format_throws_java_error(spark_session, tmpdir, monkeypatch):
    csv_file = tmpdir.mkdir('data').join('beestest_yyyy_mm_dd_hh_mm_ss_ffffff.csv')
    csv_text = "wrong,header,values\r" \
               "hello,world,testing\r"
    csv_file.write(csv_text)
    mock_logger = mock.Mock()
    # mock csv file name function to return local unit test csv file from tmp directory
    monkeypatch.setattr("jobs.input.input_processing.build_input_csv_file_name", lambda x, y, z: csv_file.strpath)
    with pytest.raises(py4j.protocol.Py4JJavaError):
        result_df = process_input_file(spark_session, mock_logger, Test.CLIENT_NAME, "unit_test", Test.JOB_RUN_ID)
        result_df.collect()


def test_transform_input_csv_returns_rows_with_single_leadid(spark_session):
    # record_id, phone_1, phone_2, phone_3, phone_4, email_1, email_2, email_3, lead_1, lead_2, lead_3, as_of_time
    raw_hash_rows = [
        (1, None, None, None, None, None, None, None, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', None, None, '1506844800'),
        (2, None, None, None, None, None, None, None, 'LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', None, None, '1506844800')]
    input_csv_data_frame = spark_session.createDataFrame(raw_hash_rows, input_csv_schema())
    result_data_frame = transform_input_csv(input_csv_data_frame)
    extracted_row_values = [[i.record_id, i.input_id_raw, i.input_id_type, i.has_error, i.error_message] for i
                            in result_data_frame.select(
            result_data_frame.record_id, result_data_frame.input_id_raw, result_data_frame.input_id_type,
            result_data_frame.has_error, result_data_frame.error_message).collect()]
    expected_rows = [
        [1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', IdentifierTypes.LEADID, False, None],
        [2, 'LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', IdentifierTypes.LEADID, False, None]
    ]
    assert extracted_row_values == expected_rows


def test_transform_input_csv_returns_rows_with_single_email(spark_session):
    # record_id, phone_1, phone_2, phone_3, phone_4, email_1, email_2, email_3, lead_1, lead_2, lead_3, as_of_time
    raw_hash_rows = [
        (1, None, None, None, None, 'EE1111', None, None, None, None, None, '1506844800'),
        (2, None, None, None, None, 'EE2222', None, None, None, None, None, '1506844800')]
    input_csv_data_frame = spark_session.createDataFrame(raw_hash_rows, input_csv_schema())
    result_data_frame = transform_input_csv(input_csv_data_frame)
    extracted_row_values = [[i.record_id, i.input_id_raw, i.input_id_type, i.has_error, i.error_message] for i
                            in result_data_frame.select(
            result_data_frame.record_id, result_data_frame.input_id_raw, result_data_frame.input_id_type,
            result_data_frame.has_error, result_data_frame.error_message).collect()]
    expected_rows = [
        [1, 'EE1111', IdentifierTypes.EMAIL, False, None],
        [2, 'EE2222', IdentifierTypes.EMAIL, False, None]
    ]
    assert extracted_row_values == expected_rows


def test_transform_input_csv_returns_rows_with_single_phone(spark_session):
    # record_id, phone_1, phone_2, phone_3, phone_4, email_1, email_2, email_3, lead_1, lead_2, lead_3, as_of_time
    raw_hash_rows = [
        (1, 'PP1111', None, None, None, None, None, None, None, None, None, '1506844800'),
        (2, 'PP2222', None, None, None, None, None, None, None, None, None, '1506844800')]
    input_csv_data_frame = spark_session.createDataFrame(raw_hash_rows, input_csv_schema())
    result_data_frame = transform_input_csv(input_csv_data_frame)
    extracted_row_values = [[i.record_id, i.input_id_raw, i.input_id_type, i.has_error, i.error_message] for i
                            in result_data_frame.select(
            result_data_frame.record_id, result_data_frame.input_id_raw, result_data_frame.input_id_type,
            result_data_frame.has_error, result_data_frame.error_message).collect()]
    expected_rows = [
        [1, 'PP1111', IdentifierTypes.PHONE, False, None],
        [2, 'PP2222', IdentifierTypes.PHONE, False, None]
    ]
    assert extracted_row_values == expected_rows


def test_transform_input_csv_returns_rows_with_multiple_value(spark_session):
    # record_id, phone_1, phone_2, phone_3, phone_4, email_1, email_2, email_3, lead_1, lead_2, lead_3, as_of_time
    raw_hash_rows = [
        (1, 'PP1111', 'PP2222', None, None, 'EE1111', 'EE2222', None, 'LL1111', 'LL2222', None, '1506844800'),
        (2, 'PP3333', None, None, None, None, None, None, None, None, None, '1506844800')]
    input_csv_data_frame = spark_session.createDataFrame(raw_hash_rows, input_csv_schema())
    result_data_frame = transform_input_csv(input_csv_data_frame)
    extracted_row_values = [[i.record_id, i.input_id_raw, i.input_id_type, i.has_error, i.error_message] for i
                            in result_data_frame.select(
            result_data_frame.record_id, result_data_frame.input_id_raw, result_data_frame.input_id_type,
            result_data_frame.has_error, result_data_frame.error_message).collect()]
    expected_rows = [
        [1, 'PP1111', IdentifierTypes.PHONE, False, None],
        [1, 'PP2222', IdentifierTypes.PHONE, False, None],
        [1, 'EE1111', IdentifierTypes.EMAIL, False, None],
        [1, 'EE2222', IdentifierTypes.EMAIL, False, None],
        [1, 'LL1111', IdentifierTypes.LEADID, False, None],
        [1, 'LL2222', IdentifierTypes.LEADID, False, None],
        [2, 'PP3333', IdentifierTypes.PHONE, False, None]
    ]
    assert extracted_row_values == expected_rows


def input_csv_transformed_schema():
    # record_id, input_id_raw, input_id_type, has_error, error_message
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), True),
         StructField(InputColumnNames.INPUT_ID_RAW, StringType(), True),
         StructField(InputColumnNames.INPUT_ID_TYPE, StringType(), True),
         StructField(InputColumnNames.HAS_ERROR, BooleanType(), True),
         StructField(InputColumnNames.ERROR_MESSAGE, StringType(), True)])
