import pytest
from jobs.scoring.scoring import score_file, flatten_subcategories
from shared.utilities import *
from pyspark.sql.types import StructField, StructType, StringType, LongType


# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


class ClassificationCategoryAbbreviations:
    AUTO_SALES = 'auto_sales'
    EDUCATION = 'education'
    INSURANCE = 'insurance'
    FINANCIAL_SERVICES = 'financial_services'
    REAL_ESTATE = 'real_estate'
    JOBS = 'jobs'
    LEGAL = 'legal'
    HOME_SERVICES = 'home_services'
    OTHER = 'other'


def test_flatten_subcategories_returns_rows_with_key_and_abbreviation(spark_session):
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = flatten_subcategories(subcategory_df)
    expected_results = [
        [1, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [2, 0, 1, 0, 0, 0, 0, 0, 0, 0],
        [3, 0, 0, 1, 0, 0, 0, 0, 0, 0],
        [4, 0, 0, 0, 1, 0, 0, 0, 0, 0],
        [5, 0, 0, 0, 0, 1, 0, 0, 0, 0],
        [6, 0, 0, 0, 0, 0, 1, 0, 0, 0],
        [7, 0, 0, 0, 0, 0, 0, 1, 0, 0],
        [8, 0, 0, 0, 0, 0, 0, 0, 1, 0],
        [9, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    ]
    expected_cols = [COL_NAME_CLASSIF_SUBCATEGORY_KEY, ClassificationCategoryAbbreviations.AUTO_SALES,
                     ClassificationCategoryAbbreviations.EDUCATION, ClassificationCategoryAbbreviations.INSURANCE,
                     ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
                     ClassificationCategoryAbbreviations.REAL_ESTATE,
                     ClassificationCategoryAbbreviations.JOBS, ClassificationCategoryAbbreviations.LEGAL,
                     ClassificationCategoryAbbreviations.HOME_SERVICES, ClassificationCategoryAbbreviations.OTHER]
    extracted_row_values = extract_rows_for_col(result_df, expected_cols, COL_NAME_CLASSIF_SUBCATEGORY_KEY)
    assert sorted(result_df.schema.names) == sorted(expected_cols)
    assert expected_results == extracted_row_values


def test_score_file_transforms_to_single_values(spark_session):
    input_rows = [
        (10, 'LLAAAAAA', 1),
        (20, 'LLBBBBBB', 2),
        (30, 'LLCCCCCC', 3),
        (40, 'LLDDDDDD', 4),
        (50, 'LLEEEEEE', 5),
        (60, 'LLFFFFFF', 6),
        (70, 'LLGGGGGG', 7),
        (80, 'LLHHHHHH', 8),
        (90, 'LLIIIIII', 9)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = score_file(subcategory_df, input_df)
    expected_results = [
        [10, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [20, 0, 1, 0, 0, 0, 0, 0, 0, 0],
        [30, 0, 0, 1, 0, 0, 0, 0, 0, 0],
        [40, 0, 0, 0, 1, 0, 0, 0, 0, 0],
        [50, 0, 0, 0, 0, 1, 0, 0, 0, 0],
        [60, 0, 0, 0, 0, 0, 1, 0, 0, 0],
        [70, 0, 0, 0, 0, 0, 0, 1, 0, 0],
        [80, 0, 0, 0, 0, 0, 0, 0, 1, 0],
        [90, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    ]
    col_names = [COL_NAME_RECORD_ID, ClassificationCategoryAbbreviations.AUTO_SALES,
                 ClassificationCategoryAbbreviations.EDUCATION,
                 ClassificationCategoryAbbreviations.INSURANCE, ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
                 ClassificationCategoryAbbreviations.REAL_ESTATE, ClassificationCategoryAbbreviations.JOBS,
                 ClassificationCategoryAbbreviations.LEGAL, ClassificationCategoryAbbreviations.HOME_SERVICES,
                 ClassificationCategoryAbbreviations.OTHER]
    extracted_row_values = extract_rows_for_col(result_df, col_names, COL_NAME_RECORD_ID)
    assert expected_results == extracted_row_values


def test_score_file_returns_category_abbrev_column_values(spark_session):
    input_rows = [(10, 'LLAAAAAA', 1)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = score_file(subcategory_df, input_df)
    col_names = [COL_NAME_RECORD_ID, ClassificationCategoryAbbreviations.AUTO_SALES,
                 ClassificationCategoryAbbreviations.EDUCATION,
                 ClassificationCategoryAbbreviations.INSURANCE, ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
                 ClassificationCategoryAbbreviations.REAL_ESTATE, ClassificationCategoryAbbreviations.JOBS,
                 ClassificationCategoryAbbreviations.LEGAL, ClassificationCategoryAbbreviations.HOME_SERVICES,
                 ClassificationCategoryAbbreviations.OTHER]
    assert sorted(result_df.schema.names) == sorted(col_names)


def test_score_file_with_no_classification_in_row(spark_session):
    input_rows = [
        (10, 'LLAAAAAA', 1),
        (20, 'LLBBBBBB', None),  # no classification found
        (30, 'LLCCCCCC', 3)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = score_file(subcategory_df, input_df)
    expected_results = [
        [10, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [20, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # row should be all zeros
        [30, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    ]
    col_names = [COL_NAME_RECORD_ID, ClassificationCategoryAbbreviations.AUTO_SALES,
                 ClassificationCategoryAbbreviations.EDUCATION,
                 ClassificationCategoryAbbreviations.INSURANCE, ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
                 ClassificationCategoryAbbreviations.REAL_ESTATE, ClassificationCategoryAbbreviations.JOBS,
                 ClassificationCategoryAbbreviations.LEGAL, ClassificationCategoryAbbreviations.HOME_SERVICES,
                 ClassificationCategoryAbbreviations.OTHER]
    extracted_row_values = extract_rows_for_col(result_df, col_names, COL_NAME_RECORD_ID)
    assert expected_results == extracted_row_values


def test_score_file_with_empty_input_file(spark_session):
    input_rows = []
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = score_file(subcategory_df, input_df)
    assert result_df.count() == 0


def define_classification_subcategory_df(spark_session):
    raw_hash_rows = [(1, 1, ClassificationCategoryAbbreviations.AUTO_SALES, 'Auto Sales', 0),
                     (2, 1, ClassificationCategoryAbbreviations.EDUCATION, 'Education', 0),
                     (3, 1, ClassificationCategoryAbbreviations.INSURANCE, 'Insurance', 0),
                     (4, 1, ClassificationCategoryAbbreviations.FINANCIAL_SERVICES, 'Financial Services', 0),
                     (5, 1, ClassificationCategoryAbbreviations.REAL_ESTATE, 'Real Estate', 0),
                     (6, 1, ClassificationCategoryAbbreviations.JOBS, 'Jobs', 0),
                     (7, 1, ClassificationCategoryAbbreviations.LEGAL, 'Legal', 0),
                     (8, 1, ClassificationCategoryAbbreviations.HOME_SERVICES, 'Home Services', 0),
                     (9, 1, ClassificationCategoryAbbreviations.OTHER, 'Other', 0)]
    col_names = [COL_NAME_CLASSIF_SUBCATEGORY_KEY, COL_NAME_CLASSIF_CATEGORY_KEY, COL_NAME_SUBCATEGORY_NAME,
                 COL_NAME_DISPLAY_NAME, COL_NAME_INSERTED_TIMESTAMP]
    return spark_session.createDataFrame(raw_hash_rows, col_names)


def scoring_input_schema():
    # record_id, input_id, classif_subcategory_key
    csv_schema = StructType(
        [StructField(COL_NAME_RECORD_ID, LongType(), False),
         StructField(COL_NAME_INPUT_ID, StringType(), True),
         StructField(COL_NAME_CLASSIF_SUBCATEGORY_KEY, StringType(), True)])
    return csv_schema


def extract_rows_for_col(data_frame, col_names, order_by_column):
    # list comprehension is only way I can think of to make this easy
    # get Row objects and translate to Dict type
    rows_as_dicts = [i.asDict() for i in data_frame.select(col_names).orderBy(order_by_column).collect()]

    # from Dict get values in same order as column name COLUMN order
    list_values = [[row_dict.get(col_name) for col_name in col_names] for row_dict in rows_as_dicts]
    return list_values
