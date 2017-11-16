import pytest
from pyspark.sql.types import StructField, StructType, StringType, LongType, BooleanType

from jobs.pii_hashing.pii_hashing import get_hash_mapping_schema_location, \
    select_distinct_raw_hash_values_for_phones_emails, populate_all_raw_inputs, lookup_canonical_hashes, \
    transform_lead_values_input_id_column, join_canonical_hash_values_to_phones_emails, PiiInternalColumnNames
from shared.constants import Environments, IdentifierTypes, InputColumnNames, HashMappingColumnNames

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_get_hash_mapping_schema_location_returns_correct_dev_env_name():
    full_name = get_hash_mapping_schema_location(Environments.DEV)
    assert full_name == "s3://jornaya-dev-us-east-1-fdl/hash_mapping"


def test_get_hash_mapping_schema_location_returns_correct_local_env_name():
    full_name = get_hash_mapping_schema_location(Environments.LOCAL)
    assert full_name == "../samples/hash_mapping"


def test_get_hash_mapping_schema_location_returns_correct_prod_env_name():
    full_name = get_hash_mapping_schema_location(Environments.PROD)
    assert full_name == "s3://jornaya-prod-us-east-1-fdl/hash_mapping"


def test_get_hash_mapping_schema_location_returns_correct_qa_env_name():
    full_name = get_hash_mapping_schema_location(Environments.QA)
    assert full_name == "s3://jornaya-qa-us-east-1-fdl/hash_mapping"


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
    result_df = populate_all_raw_inputs(raw_data_frame, hash_mapping_df)
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
    extracted_row_values = extract_rows_for_col(result_df, expected_col_names, InputColumnNames.RECORD_ID)
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
    phones_emails_df = select_distinct_raw_hash_values_for_phones_emails(raw_data_frame)
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
    result_df = lookup_canonical_hashes(hash_mapping_df, raw_data_frame)
    expected_col_names = [PiiInternalColumnNames.RAW_HASH_VALUE, HashMappingColumnNames.CANONICAL_HASH_VALUE]
    expected_rows = [
        ["PPAAAAAA", "CPAAAAAA"],
        ["EEGGGGGG", "CEGGGGGG"],
        ["EEFFFFFF", "CEFFFFFF"],
        ["EEHHHHHH", None]]
    extracted_row_values = extract_rows_for_col(result_df, expected_col_names, PiiInternalColumnNames.RAW_HASH_VALUE)
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
    leads_df = transform_lead_values_input_id_column(raw_data_frame)
    expected_col_names = [InputColumnNames.RECORD_ID, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE,
                          InputColumnNames.INPUT_ID]
    expected_rows = [
        [2, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB", IdentifierTypes.LEADID, "LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB"],
        [2, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP", IdentifierTypes.LEADID, "LLPPPPPP-PPPP-PPPP-PPPP-PPPPPPPPPPPP"],
        [4, "LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA", IdentifierTypes.LEADID, "LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"]]
    extracted_row_values = extract_rows_for_col(leads_df, expected_col_names, InputColumnNames.RECORD_ID)
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
    hash_mapping_df = spark_session.createDataFrame(hash_mapping_rows, (PiiInternalColumnNames.RAW_HASH_VALUE,
                                                                        HashMappingColumnNames.CANONICAL_HASH_VALUE))
    email_phones_df = join_canonical_hash_values_to_phones_emails(raw_data_frame, hash_mapping_df)
    expected_col_names = [InputColumnNames.RECORD_ID, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE,
                          InputColumnNames.INPUT_ID]
    expected_rows = [
        [1, "PPAAAAAA", IdentifierTypes.PHONE, "CPAAAAAA"],
        [2, "EEGGGGGG", IdentifierTypes.EMAIL, "CEGGGGGG"],
        [3, "EEFFFFFF", IdentifierTypes.EMAIL, "CEFFFFFF"],
        [4, "EEFFFFFF", IdentifierTypes.EMAIL, "CEFFFFFF"],
        [5, "PPAAAAAA", IdentifierTypes.PHONE, "CPAAAAAA"]]
    extracted_row_values = extract_rows_for_col(email_phones_df, expected_col_names, InputColumnNames.RECORD_ID)
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
        [StructField(PiiInternalColumnNames.RAW_HASH_VALUE, StringType(), True)])


def hash_mapping_schema():
    return StructType(
        [StructField(HashMappingColumnNames.CANONICAL_HASH_VALUE, StringType(), True),
         StructField(HashMappingColumnNames.HASH_VALUE, StringType(), True)])


# TODO: extract this, DRY
def extract_rows_for_col(data_frame, col_names, order_by_column):
    # list comprehension is only way I can think of to make this easy
    # get Row objects and translate to Dict type
    rows_as_dicts = [i.asDict() for i in data_frame.select(col_names).orderBy(order_by_column).collect()]

    # from Dict get values in same order as column name COLUMN order
    return [[row_dict.get(col_name) for col_name in col_names] for row_dict in rows_as_dicts]
