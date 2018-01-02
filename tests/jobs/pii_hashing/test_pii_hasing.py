import pytest
from pyspark.sql.types import StructField, StructType, StringType, LongType, BooleanType

from jobs.pii_hashing import pii_hashing
from shared.constants import IdentifierTypes, InputColumnNames, HashMappingColumnNames
from tests.helpers import extract_rows_for_col_with_order

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_populate_all_raw_inputs_transforms_all_types(spark_session):
    # record_id, input_id_raw, input_id_type, has_error, error_messages
    raw_input_rows = [
        (1, "PPAAAAAA", IdentifierTypes.PHONE, False, None),
        (2, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB", IdentifierTypes.LEADID, False, None),
        (2, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP", IdentifierTypes.LEADID, False, None),
        (2, "EEGGGGGG", IdentifierTypes.EMAIL, False, None),
        (3, "EEFFFFFF", IdentifierTypes.EMAIL, False, None),
        (4, "EEFFFFFF", IdentifierTypes.EMAIL, False, None),
        (5, "PPAAAAAA", IdentifierTypes.PHONE, False, None),
        (6, "EEHHHHHH", IdentifierTypes.EMAIL, False, None)]
    raw_data_frame = spark_session.createDataFrame(raw_input_rows, input_csv_transformed_schema())

    # build hash mapping dataframe (canonical_hash_value, hash_value)
    raw_hash_map_rows = [
        ("CPAAAAAA", "PPAAAAAA"),
        ("CEGGGGGG", "EEGGGGGG"),
        ("CEFFFFFF", "EEFFFFFF")]
    hash_mapping_df = spark_session.createDataFrame(raw_hash_map_rows, [HashMappingColumnNames.CANONICAL_HASH_VALUE,
                                                                        HashMappingColumnNames.HASH_VALUE])
    result_df = pii_hashing.populate_all_raw_inputs(raw_data_frame, hash_mapping_df)
    expected_col_names = [InputColumnNames.RECORD_ID, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE,
                          InputColumnNames.INPUT_ID]
    expected_rows = [
        [1, "PPAAAAAA", IdentifierTypes.PHONE, "CPAAAAAA"],
        [2, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB", IdentifierTypes.LEADID, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB"],
        [2, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP", IdentifierTypes.LEADID, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP"],
        [2, "EEGGGGGG", IdentifierTypes.EMAIL, "CEGGGGGG"],
        [3, "EEFFFFFF", IdentifierTypes.EMAIL, "CEFFFFFF"],
        [4, "EEFFFFFF", IdentifierTypes.EMAIL, "CEFFFFFF"],
        [5, "PPAAAAAA", IdentifierTypes.PHONE, "CPAAAAAA"],
        [6, "EEHHHHHH", IdentifierTypes.EMAIL, None]]
    extracted_row_values = extract_rows_for_col_with_order(result_df, expected_col_names, InputColumnNames.RECORD_ID)
    assert sorted(extracted_row_values) == sorted(expected_rows)


def test_select_distinct_raw_hash_values_for_phones_emails_returns_distinct_list(spark_session):
    raw_input_rows = [
        (1, "PPAAAAAA", IdentifierTypes.PHONE, False, None),
        (2, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB", IdentifierTypes.LEADID, False, None),
        (2, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP", IdentifierTypes.LEADID, False, None),
        (2, "EEGGGGGG", IdentifierTypes.EMAIL, False, None),
        (3, "EEFFFFFF", IdentifierTypes.EMAIL, False, None),
        (4, "EEFFFFFF", IdentifierTypes.EMAIL, False, None),
        (5, "PPAAAAAA", IdentifierTypes.PHONE, False, None)]
    raw_data_frame = spark_session.createDataFrame(raw_input_rows, input_csv_transformed_schema())
    phones_emails_df = pii_hashing.select_distinct_raw_hash_values_for_phones_emails(raw_data_frame)
    extracted_row_values = [i.raw_hash_value for i
                            in phones_emails_df.select(phones_emails_df.raw_hash_value).collect()]
    expected_rows = [
        "PPAAAAAA",
        "EEGGGGGG",
        "EEFFFFFF"]
    assert sorted(extracted_row_values) == sorted(expected_rows)


def test_lookup_canonical_hashes_returns_only_matching_values(spark_session):
    raw_input_rows = [["PPAAAAAA"], ["EEGGGGGG"], ["EEFFFFFF"], ["EEHHHHHH"]]
    raw_data_frame = spark_session.createDataFrame(raw_input_rows, distinct_raw_hash_schema())
    # build hash mapping dataframe
    hash_mapping_rows = [
        ("CPAAAAAA", "PPAAAAAA"),
        ("CEGGGGGG", "EEGGGGGG"),
        ("CEFFFFFF", "EEFFFFFF")]
    hash_mapping_df = spark_session.createDataFrame(hash_mapping_rows, hash_mapping_schema())
    result_df = pii_hashing.lookup_canonical_hashes(hash_mapping_df, raw_data_frame)
    expected_col_names = [pii_hashing.PiiInternalColumnNames.RAW_HASH_VALUE,
                          HashMappingColumnNames.CANONICAL_HASH_VALUE]
    expected_rows = [
        ["PPAAAAAA", "CPAAAAAA"],
        ["EEGGGGGG", "CEGGGGGG"],
        ["EEFFFFFF", "CEFFFFFF"],
        ["EEHHHHHH", None]]
    extracted_row_values = extract_rows_for_col_with_order(result_df,
                                                           expected_col_names,
                                                           pii_hashing.PiiInternalColumnNames.RAW_HASH_VALUE)
    assert sorted(extracted_row_values) == sorted(expected_rows)


def test_transform_lead_values_input_id_column_transforms_only_leads(spark_session):
    raw_input_rows = [
        (1, "PPAAAAAA", IdentifierTypes.PHONE, False, None),
        (2, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB", IdentifierTypes.LEADID, False, None),
        (2, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP", IdentifierTypes.LEADID, False, None),
        (2, "EEGGGGGG", IdentifierTypes.EMAIL, False, None),
        (3, "EEFFFFFF", IdentifierTypes.EMAIL, False, None),
        (4, "EEFFFFFF", IdentifierTypes.EMAIL, False, None),
        (4, "LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA", IdentifierTypes.LEADID, False, None),
        (5, "PPAAAAAA", IdentifierTypes.PHONE, False, None)]
    raw_data_frame = spark_session.createDataFrame(raw_input_rows, input_csv_transformed_schema())
    leads_df = pii_hashing.transform_lead_values_input_id_column(raw_data_frame)
    expected_col_names = [InputColumnNames.RECORD_ID, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE,
                          InputColumnNames.INPUT_ID]
    expected_rows = [
        [2, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB", IdentifierTypes.LEADID, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB"],
        [2, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP", IdentifierTypes.LEADID, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP"],
        [4, "LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA", IdentifierTypes.LEADID, "LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"]]
    extracted_row_values = extract_rows_for_col_with_order(leads_df, expected_col_names, InputColumnNames.RECORD_ID)
    assert sorted(extracted_row_values) == sorted(expected_rows)


def test_join_canonical_hash_values_to_phones_emails(spark_session):
    raw_input_rows = [
        (1, "PPAAAAAA", IdentifierTypes.PHONE, False, None),
        (2, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB", IdentifierTypes.LEADID, False, None),
        (2, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP", IdentifierTypes.LEADID, False, None),
        (2, "EEGGGGGG", IdentifierTypes.EMAIL, False, None),
        (3, "EEFFFFFF", IdentifierTypes.EMAIL, False, None),
        (4, "EEFFFFFF", IdentifierTypes.EMAIL, False, None),
        (4, "LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA", IdentifierTypes.LEADID, False, None),
        (5, "PPAAAAAA", IdentifierTypes.PHONE, False, None)]
    raw_data_frame = spark_session.createDataFrame(raw_input_rows, input_csv_transformed_schema())
    # build hash mapping dataframe
    hash_mapping_rows = [
        ("PPAAAAAA", "CPAAAAAA"),
        ("EEGGGGGG", "CEGGGGGG"),
        ("EEFFFFFF", "CEFFFFFF")]
    hash_mapping_df = spark_session.createDataFrame(hash_mapping_rows,
                                                    (pii_hashing.PiiInternalColumnNames.RAW_HASH_VALUE,
                                                     HashMappingColumnNames.CANONICAL_HASH_VALUE))
    email_phones_df = pii_hashing.join_canonical_hash_values_to_phones_emails(raw_data_frame, hash_mapping_df)
    expected_col_names = [InputColumnNames.RECORD_ID, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE,
                          InputColumnNames.INPUT_ID]
    expected_rows = [
        [1, "PPAAAAAA", IdentifierTypes.PHONE, "CPAAAAAA"],
        [2, "EEGGGGGG", IdentifierTypes.EMAIL, "CEGGGGGG"],
        [3, "EEFFFFFF", IdentifierTypes.EMAIL, "CEFFFFFF"],
        [4, "EEFFFFFF", IdentifierTypes.EMAIL, "CEFFFFFF"],
        [5, "PPAAAAAA", IdentifierTypes.PHONE, "CPAAAAAA"]]
    extracted_row_values = extract_rows_for_col_with_order(email_phones_df,
                                                           expected_col_names,
                                                           InputColumnNames.RECORD_ID)
    assert sorted(extracted_row_values) == sorted(expected_rows)


def input_csv_transformed_schema():
    # record_id, input_id_raw, input_id_type, has_error, error_message
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), True),
         StructField(InputColumnNames.INPUT_ID_RAW, StringType(), True),
         StructField(InputColumnNames.INPUT_ID_TYPE, StringType(), True),
         StructField(InputColumnNames.HAS_ERROR, BooleanType(), True),
         StructField(InputColumnNames.ERROR_MESSAGE, StringType(), True)])


def distinct_raw_hash_schema():
    return StructType(
        [StructField(pii_hashing.PiiInternalColumnNames.RAW_HASH_VALUE, StringType(), True)])


def hash_mapping_schema():
    return StructType(
        [StructField(HashMappingColumnNames.CANONICAL_HASH_VALUE, StringType(), True),
         StructField(HashMappingColumnNames.HASH_VALUE, StringType(), True)])
