from unittest import mock

import py4j
import pytest
from pyspark.sql.types import StructField, StructType, StringType, LongType, BooleanType
from pyspark.sql.utils import AnalysisException

from jobs.input import input_processing as input
from shared.constants import Environments, InputColumnNames, IdentifierTypes, Test, GenericColumnNames

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_build_input_csv_file_name_returns_correct_dev_env_name():
    full_name = input.build_input_csv_file_name(Environments.DEV, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == 's3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_build_input_csv_file_name_returns_correct_qa_env_name():
    full_name = input.build_input_csv_file_name(Environments.QA, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == 's3://jornaya-qa-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_build_input_csv_file_name_returns_correct_staging_env_name():
    full_name = input.build_input_csv_file_name(Environments.STAGING, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == 's3://jornaya-staging-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_build_input_csv_file_name_returns_correct_prod_env_name():
    full_name = input.build_input_csv_file_name(Environments.PROD, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == 's3://jornaya-prod-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_build_input_csv_file_name_returns_local_env_name_as_samples_directory():
    full_name = input.build_input_csv_file_name(Environments.LOCAL, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert full_name == '../samples/app_data/beestest/beestest_2018_01_02/input/beestest_2018_01_02.csv'


def test_transform_input_csv_returns_empty_data_frame_when_no_data_present(spark_session):
    input_csv_data_frame = spark_session.createDataFrame([], input.input_csv_schema())
    result_data_frame = input.transform_input_csv(input_csv_data_frame)
    extracted_row_values = [[i.record_id, i.input_id_raw, i.input_id_type] for i in result_data_frame.select(
        result_data_frame.record_id, result_data_frame.input_id_raw, result_data_frame.input_id_type).collect()]
    assert extracted_row_values == []


def test_transform_input_csv_returns_proper_data_frame_schema_columns(spark_session):
    input_csv_data_frame = spark_session.createDataFrame([], input.input_csv_schema())
    result_data_frame = input.transform_input_csv(input_csv_data_frame)
    assert sorted(result_data_frame.schema.names) == sorted(input_csv_transformed_schema().names)


def test_load_csv_file_loads_file(spark_session, tmpdir):
    csv_file = tmpdir.mkdir('data').join('beestest_yyyy_mm_dd_hh_mm_ss_ffffff.csv')
    csv_text = "recordid,phone01,phone02,phone03,phone04,email01,email02,email03,leadid01,leadid02,leadid03,asof\r" \
               "1,,,,,,,,LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA,,,\r" \
               "2,,,,,,,,LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB,,,\r"
    csv_file.write(csv_text)
    loaded_df = input.load_csv_file(spark_session, csv_file.strpath, input.input_csv_schema())
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
    result_df = input.process_input_file(spark_session, mock_logger, Test.CLIENT_NAME, "unit_test", Test.JOB_RUN_ID)
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
        input.process_input_file(spark_session, mock_logger, Test.CLIENT_NAME, "unit_test", Test.JOB_RUN_ID)


def test_process_input_file_with_invalid_csv_file_format_throws_java_error(spark_session, tmpdir, monkeypatch):
    csv_file = tmpdir.mkdir('data').join('beestest_yyyy_mm_dd_hh_mm_ss_ffffff.csv')
    csv_text = "wrong,header,values\r" \
               "hello,world,testing\r"
    csv_file.write(csv_text)
    mock_logger = mock.Mock()
    # mock csv file name function to return local unit test csv file from tmp directory
    monkeypatch.setattr("jobs.input.input_processing.build_input_csv_file_name", lambda x, y, z: csv_file.strpath)
    with pytest.raises(py4j.protocol.Py4JJavaError):
        result_df = input.process_input_file(spark_session, mock_logger, Test.CLIENT_NAME, "unit_test", Test.JOB_RUN_ID)
        result_df.collect()


def test_transform_input_csv_returns_rows_with_single_leadid(spark_session):
    # record_id, phone_1, phone_2, phone_3, phone_4, email_1, email_2, email_3, lead_1, lead_2, lead_3, as_of_time
    raw_hash_rows = [
        (1, None, None, None, None, None, None, None, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', None, None, '1506844800'),
        (2, None, None, None, None, None, None, None, 'LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', None, None, '1506844800')]
    input_csv_data_frame = spark_session.createDataFrame(raw_hash_rows, input.input_csv_schema())
    result_data_frame = input.transform_input_csv(input_csv_data_frame)
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
    input_csv_data_frame = spark_session.createDataFrame(raw_hash_rows, input.input_csv_schema())
    result_data_frame = input.transform_input_csv(input_csv_data_frame)
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
    input_csv_data_frame = spark_session.createDataFrame(raw_hash_rows, input.input_csv_schema())
    result_data_frame = input.transform_input_csv(input_csv_data_frame)
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
    input_csv_data_frame = spark_session.createDataFrame(raw_hash_rows, input.input_csv_schema())
    result_data_frame = input.transform_input_csv(input_csv_data_frame)
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


def test_create_three_column_row_returns_expected_column_values(spark_session):
    row = input.create_three_column_row(spark_session, GenericColumnNames.PHONE, 25, '')
    assert row.collect()[0][0] == GenericColumnNames.PHONE
    assert row.collect()[0][1] == 25
    assert row.collect()[0][2] == ''


def test_calculate_identifier_coverage_returns_100_percent_coverage():
    identifier_coverage = input.calculate_identifier_coverage(17, 17)
    assert '100 %' == identifier_coverage


def test_calculate_identifier_coverage_returns_correct_coverage_percent():
    identifier_coverage = input.calculate_identifier_coverage(12, 17)
    assert '71 %' == identifier_coverage


def test_validate_column_names_inserts_email_if_not_found(spark_session):
    row = [(1, 'abcd', None, None, None)]
    dataframe = spark_session.createDataFrame(row, input_summary_test_schema())
    dataframe_no_email = dataframe.drop(GenericColumnNames.EMAIL)
    assert GenericColumnNames.EMAIL not in dataframe_no_email.schema.names
    validated_dataframe = input.validate_column_names(dataframe_no_email)
    assert GenericColumnNames.EMAIL in validated_dataframe.schema.names


def test_validate_column_names_inserts_phone_if_not_found(spark_session):
    row = [(1, 'abcd', None, None, None)]
    dataframe = spark_session.createDataFrame(row, input_summary_test_schema())
    dataframe_no_phone = dataframe.drop(GenericColumnNames.PHONE)
    assert GenericColumnNames.PHONE not in dataframe_no_phone.schema.names
    validated_dataframe = input.validate_column_names(dataframe_no_phone)
    assert GenericColumnNames.PHONE in validated_dataframe.schema.names


def test_validate_column_names_inserts_leadid_if_not_found(spark_session):
    row = [(1, 'abcd', None, None, None)]
    dataframe = spark_session.createDataFrame(row, input_summary_test_schema())
    dataframe_no_leadid = dataframe.drop(GenericColumnNames.LEAD_ID)
    assert GenericColumnNames.LEAD_ID not in dataframe_no_leadid.schema.names
    validated_dataframe = input.validate_column_names(dataframe_no_leadid)
    assert GenericColumnNames.LEAD_ID in validated_dataframe.schema.names


def test_get_unique_identifier_count_for_phones(spark_session):
    row = [(1, 'abc', 1), (2, 'abc', 1), (3, 'ghi', 1),
           (4, 'jkl', None), (5, 'mno', 1), (6, 'mno', 1), (7, 'stu', 1), (8, 'vwx', None)]
    dataframe = spark_session.createDataFrame(row, create_record_id_schema(GenericColumnNames.PHONE))
    unique_count = input.get_unique_identifier_count(dataframe,
                                                     [InputColumnNames.INPUT_ID_RAW, GenericColumnNames.PHONE],
                                                     GenericColumnNames.PHONE)
    assert 4 == unique_count


def test_get_unique_identifier_count_for_emails(spark_session):
    row = [(1, 'abc', 1), (2, 'def', 1), (3, 'ghi', 1),
           (4, 'jkl', None), (5, 'mno', 1), (6, 'mno', 1), (7, 'stu', 1), (8, 'vwx', None)]
    dataframe = spark_session.createDataFrame(row, create_record_id_schema(GenericColumnNames.EMAIL))
    unique_count = input.get_unique_identifier_count(dataframe,
                                                     [InputColumnNames.INPUT_ID_RAW, GenericColumnNames.EMAIL],
                                                     GenericColumnNames.EMAIL)
    assert 5 == unique_count


def test_get_unique_identifier_count_for_leads(spark_session):
    row = [(1, 'abc', 1), (2, 'abc', 1), (3, 'ghi', 1),
           (4, 'jkl', None), (5, 'mno', 1), (6, 'mno', 1), (7, 'stu', None), (8, 'vwx', None)]
    dataframe = spark_session.createDataFrame(row, create_record_id_schema(GenericColumnNames.LEAD_ID))
    unique_count = input.get_unique_identifier_count(dataframe,
                                                     [InputColumnNames.INPUT_ID_RAW, GenericColumnNames.LEAD_ID],
                                                     GenericColumnNames.LEAD_ID)
    assert 3 == unique_count


def test_get_record_count_for_identifier_passing_phone(spark_session):
    row = [(1, 'abc', 1), (1, 'abc', 1), (1, 'ghi', 1),
           (2, 'jkl', None), (5, 'mno', 1), (6, 'mno', 1), (7, 'stu', None), (8, 'vwx', None)]
    dataframe = spark_session.createDataFrame(row, create_record_id_schema(GenericColumnNames.PHONE))
    record_count = input.get_record_count_for_identifier(dataframe, GenericColumnNames.PHONE)
    assert record_count == 3


def test_summarize_input_df_full_input_test(spark_session):
    rows = [
        ("1", "PPAAAAAA", "phone", "false", "null"),
        ("1", "EEYYYYYY", "email", "false", "null"),
        ("1", "PPSSSSSS", "phone", "false", "null"),
        ("1", "LLDDDDDD-DDDD-DDDD-DDDD-DDDDDDDDDDDD", "leadid", "false", "null"),
        ("1", "EESSSSSS", "email", "false", "null"),
        ("2", "PPBBBBBB", "phone", "false", "null"),
        ("2", "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB", "leadid", "false", "null"),
        ("2", "EEBBBBBB", "email", "false", "null"),
        ("2", "PPLLLLLL", "phone", "false", "null"),
        ("2", "PPMMMMM ", "phone", "false", "null"),
        ("3", "PPCCCCCC", "phone", "false", "null"),
        ("3", "EEAAAAAA", "email", "false", "null"),
        ("4", "EESSSSSS", "email", "false", "null"),
        ("4", "PPDDDDDD", "phone", "false", "null"),
        ("5", "PPEEEEEE", "phone", "false", "null"),
        ("5", "PPYYYYYY", "phone", "false", "null"),
        ("5", "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP", "leadid", "false", "null"),
        ("5", "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB", "leadid", "false", "null"),
        ("5", "EESSSSSS", "email", "false", "null"),
        ("6", "EEDDDDDD", "email", "false", "null"),
        ("6", "PPFFFFFF", "phone", "false", "null"),
        ("6", "LLFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", "leadid", "false", "null"),
        ("6", "EEFFFFFF", "email", "false", "null"),
        ("7", "EEGGGGGG", "email", "false", "null"),
        ("7", "PPGGGGGG", "phone", "false", "null"),
        ("8", "PPUUUUUU", "phone", "false", "null"),
        ("8", "EEHHHHHH", "email", "false", "null"),
        ("8", "EESSSSSS", "email", "false", "null"),
        ("8", "PPHHHHHH", "phone", "false", "null"),
        ("9", "EEIIIIII", "email", "false", "null"),
        ("9", "PPIIIIII", "phone", "false", "null"),
        ("9", "LLCCCCCC-CCCC-CCCC-CCCC-CCCCCCCCCCCC", "leadid", "false", "null"),
        ("10", "EEBBBBBB", "email", "false", "null"),
        ("10", "PPJJJJJJ", "phone", "false", "null"),
        ("10", "EEJJJJJJ", "email", "false", "null"),
        ("11", "LLKKKKKK-KKKK-KKKK-KKKK-KKKKKKKKKKKK", "leadid", "false", "null"),
        ("11", "PPKKKKKK", "phone", "false", "null"),
        ("11", "EEKKKKKK", "email", "false", "null"),
        ("11", "PPWWWWWW", "phone", "false", "null"),
        ("12", "LLMMMMMM-MMMM-MMMM-MMMM-MMMMMMMMMMMM", "leadid", "false", "null"),
        ("12", "EETTTTTT", "email", "false", "null"),
        ("12", "EECCCCCC", "email", "false", "null"),
        ("12", "PPNNNNNN", "phone", "false", "null"),
        ("12", "EEMMMMMM", "email", "false", "null"),
        ("13", "EENNNNNN", "email", "false", "null"),
        ("13", "PPOOOOOO", "phone", "false", "null"),
        ("13", "PPTTTTTT", "phone", "false", "null"),
        ("13", "LLOOOOOO-OOOO-OOOO-OOOO-OOOOOOOOOOOO", "leadid", "false", "null"),
        ("14", "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP", "leadid", "false", "null"),
        ("14", "PPQQQQQQ", "phone", "false", "null"),
        ("14", "EEOOOOOO", "email", "false", "null"),
        ("15", "PPZZZZZZ", "phone", "false", "null"),
        ("15", "PPRRRRRR", "phone", "false", "null"),
        ("15", "LLSSSSSS-SSSS-SSSS-SSSS-SSSSSSSSSSSS", "leadid", "false", "null"),
        ("15", "EEPPPPPP", "email", "false", "null"),
        ("15", "EERRRRRR", "email", "false", "null"),
        ("16", "PPXXXXXXXXX ", "phone", "false", "null"),
        ("16", "LLJJJJJJ-JJJJ-JJJJ-JJJJ-JJJJJJJJJJJJ", "leadid", "false", "null"),
        ("16", "EESSSSSS", "email", "false", "null"),
        ("17", "LLZZZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZZZZZZZZZ", "leadid", "false", "null"),
        ("17", "PP111111", "phone", "false", "null"),
        ("17", "PPBBBBBB", "phone", "false", "null"),
        ("17", "PP222222", "phone", "false", "null"),
        ("17", "PP333333", "phone", "false", "null"),
        ("17", "EEXXXXXXXXX ", "email", "false", "null"),
        ("17", "EEZZZZZZ", "email", "false", "null"),
        ("17", "EE111111", "email", "false", "null")]

    schema = ["record_id", "input_id_raw", "input_id_type", "has_error", "error_message"]
    input_df = spark_session.createDataFrame(rows, schema)
    res = input.summarize_input_df(spark_session, input_df)
    plain_data = res.collect()
    assert res.count() == 6
    assert plain_data[0][1] == "17"
    assert plain_data[2][1] == "Unique Identifiers"
    assert plain_data[2][2] == "Identifier Coverage"

    assert plain_data[3][0] == "Phone Numbers"
    assert plain_data[4][0] == "Email Addresses"
    assert plain_data[5][0] == "LeadiDs"

    assert plain_data[3][1] == "27"
    assert plain_data[3][2] == "100 %"
    assert plain_data[4][1] == "21"
    assert plain_data[4][2] == "100 %"
    assert plain_data[5][1] == "11"
    assert plain_data[5][2] == "71 %"


def test_summarize_input_df_with_missing_leads(spark_session):
    rows = [
        ("1", "PPAAAAAA", "phone", "false", "null"),
        ("1", "EEYYYYYY", "email", "false", "null"),
        ("1", "PPSSSSSS", "phone", "false", "null")]

    schema = ["record_id", "input_id_raw", "input_id_type", "has_error", "error_message"]
    input_df = spark_session.createDataFrame(rows, schema)
    res = input.summarize_input_df(spark_session, input_df)
    plain_data = res.collect()
    assert res.count() == 6
    assert plain_data[0][1] == "1"
    assert plain_data[3][1] == "2"
    assert plain_data[3][2] == "100 %"
    assert plain_data[4][1] == "1"
    assert plain_data[4][2] == "100 %"
    assert plain_data[5][1] == "0"
    assert plain_data[5][2] == "0 %"


def test_summarize_input_df_with_missing_phones(spark_session):
    rows = [
        ("1", "EEYYYYYY", "email", "false", "null"),
        ("1", "LLDDDDDD-DDDD-DDDD-DDDD-DDDDDDDDDDDD", "leadid", "false", "null")]

    schema = ["record_id", "input_id_raw", "input_id_type", "has_error", "error_message"]
    input_df = spark_session.createDataFrame(rows, schema)
    res = input.summarize_input_df(spark_session, input_df)
    plain_data = res.collect()
    assert res.count() == 6
    assert plain_data[0][1] == "1"
    assert plain_data[3][1] == "0"
    assert plain_data[3][2] == "0 %"
    assert plain_data[4][1] == "1"
    assert plain_data[4][2] == "100 %"
    assert plain_data[5][1] == "1"
    assert plain_data[5][2] == "100 %"


def test_summarize_input_df_with_missing_emails(spark_session):
    rows = [
        ("1", "PPAAAAAA", "phone", "false", "null"),
        ("1", "LLDDDDDD-DDDD-DDDD-DDDD-DDDDDDDDDDDD", "leadid", "false", "null"),
        ("1", "PPSSSSSS", "phone", "false", "null")]

    schema = ["record_id", "input_id_raw", "input_id_type", "has_error", "error_message"]
    input_df = spark_session.createDataFrame(rows, schema)
    res = input.summarize_input_df(spark_session, input_df)
    plain_data = res.collect()
    assert res.count() == 6
    assert plain_data[0][1] == "1"
    assert plain_data[3][1] == "2"
    assert plain_data[3][2] == "100 %"
    assert plain_data[4][1] == "0"
    assert plain_data[4][2] == "0 %"
    assert plain_data[5][1] == "1"
    assert plain_data[5][2] == "100 %"


def create_record_id_schema(col_name):
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), True),
         StructField(InputColumnNames.INPUT_ID_RAW, StringType(), True),
         StructField(col_name, StringType(), True)])


def input_csv_transformed_schema():
    # record_id, input_id_raw, input_id_type, has_error, error_message
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), True),
         StructField(InputColumnNames.INPUT_ID_RAW, StringType(), True),
         StructField(InputColumnNames.INPUT_ID_TYPE, StringType(), True),
         StructField(InputColumnNames.HAS_ERROR, BooleanType(), True),
         StructField(InputColumnNames.ERROR_MESSAGE, StringType(), True)])


def input_summary_test_schema():
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), True),
         StructField(InputColumnNames.INPUT_ID_RAW, StringType(), True),
         StructField(GenericColumnNames.LEAD_ID, StringType(), True),
         StructField(GenericColumnNames.PHONE, StringType(), True),
         StructField(GenericColumnNames.EMAIL, StringType(), True)])


def test_transform_input_csv_lowercase_leadids(spark_session):
    rows = [
    (1, 'PPAAAAAA', 'PPSSSSSS', '', '', 'EESSSSSS', 'EEYYYYYY', '', '', 'LLDDDDDD-DDDD-DDDD-DDDD-DDDDDDDDDDDD', '', ''),
    (2, 'PPBBBBBB', 'PPLLLLLL', 'PPMMMMM', '', 'EEBBBBBB', '', '', 'LLBBBBBB-BBBB-bbbb-BBBB-BBBBBBBBBBBB', '', '', ''),
    (3, 'PPCCCCCC', '', '', '', '', 'EEAAAAAA', '', '', 'lldddddd-dddd-dddd-dddd-dddddddddddd', '', '')]

    schema = ["record_id", "phone_1", "phone_2", "phone_3", "phone_4", "email_1", "email_2", "email_3", "lead_1", "lead_2", "lead_3", "asof"]
    input_df = spark_session.createDataFrame(rows, schema)
    res = input.transform_input_csv(input_df)
    res.show()
    leads = res.where(res['input_id_type'] == 'leadid')\
        .filter(res['input_id_raw'] != '')\
        .select(res['input_id_raw'])\
        .distinct().sort(res['input_id_raw'].desc())
    leads.show(10, False)
    assert leads.count() == 2
    assert leads.collect()[0][0] == 'LLDDDDDD-DDDD-DDDD-DDDD-DDDDDDDDDDDD'
    assert leads.collect()[1][0] == 'LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB'
