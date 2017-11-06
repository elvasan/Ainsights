import pytest
from pyspark.sql.types import StructField, StructType, StringType, LongType

from jobs.scoring.scoring import score_file, flatten_subcategories, join_classified_inputs_to_subcategories,\
    score_flat_results_by_frequency
from shared.utilities import ClassificationSubcategory, InputColumnNames, ClassificationCategoryAbbreviations

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


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
    expected_cols = [ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY, ClassificationCategoryAbbreviations.AUTO_SALES,
                     ClassificationCategoryAbbreviations.EDUCATION, ClassificationCategoryAbbreviations.INSURANCE,
                     ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
                     ClassificationCategoryAbbreviations.REAL_ESTATE,
                     ClassificationCategoryAbbreviations.JOBS, ClassificationCategoryAbbreviations.LEGAL,
                     ClassificationCategoryAbbreviations.HOME_SERVICES, ClassificationCategoryAbbreviations.OTHER]
    extracted_row_values = extract_rows_for_col(result_df, expected_cols,
                                                ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY)
    assert sorted(result_df.schema.names) == sorted(expected_cols)
    assert expected_results == extracted_row_values


def test_score_file_transforms_with_single_values_for_each_subcategory(spark_session):
    input_rows = [
        (10, 1),
        (20, 2),
        (30, 3),
        (40, 4),
        (50, 5),
        (60, 6),
        (70, 7),
        (80, 8),
        (90, 9)]
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
    extracted_row_values = extract_rows_for_col(result_df, all_expected_subcategory_column_names(),
                                                InputColumnNames.RECORD_ID)
    assert expected_results == extracted_row_values


def test_score_file_transforms_with_multiple_values_for_each_classification_frequency(spark_session):
    input_rows = [
        (10, 1), (10, 1), (10, 2), (10, 1), (10, 6),
        (20, 2), (20, 3), (20, 4), (20, 5), (20, 5)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = score_file(subcategory_df, input_df)
    expected_results = [
        [10, 3, 1, 0, 0, 0, 1, 0, 0, 0],
        [20, 0, 1, 1, 1, 2, 0, 0, 0, 0]
    ]
    extracted_row_values = extract_rows_for_col(result_df, all_expected_subcategory_column_names(),
                                                InputColumnNames.RECORD_ID)
    assert expected_results == extracted_row_values


def test_score_file_returns_category_abbrev_column_values(spark_session):
    input_rows = [(10, 1)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = score_file(subcategory_df, input_df)
    assert sorted(result_df.schema.names) == sorted(all_expected_subcategory_column_names())


def test_score_file_with_empty_input_file(spark_session):
    input_rows = []
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = score_file(subcategory_df, input_df)
    assert result_df.count() == 0


def test_join_classified_inputs_to_subcategories_returns_flat_results(spark_session):
    input_rows = [
        (10, 1),
        (10, 1),
        (20, 3)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    raw_subcategory_df = define_classification_subcategory_df(spark_session)
    subcategory_df = flatten_subcategories(raw_subcategory_df)
    result_df = join_classified_inputs_to_subcategories(subcategory_df, input_df)
    expected_results = [
        [10, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [10, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [20, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    ]
    extracted_row_values = extract_rows_for_col(result_df, all_expected_subcategory_column_names(), InputColumnNames.RECORD_ID)
    assert expected_results == extracted_row_values


def test_join_classified_inputs_to_subcategories_returns_zeros_for_no_classification_found(spark_session):
    input_rows = [
        (10, 1),
        (20, None),  # no classification found
        (30, 3)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    raw_subcategory_df = define_classification_subcategory_df(spark_session)
    subcategory_df = flatten_subcategories(raw_subcategory_df)
    result_df = join_classified_inputs_to_subcategories(subcategory_df, input_df)
    expected_results = [
        [10, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [20, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # row should be all zeros
        [30, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    ]
    extracted_row_values = extract_rows_for_col(result_df, all_expected_subcategory_column_names(),
                                                InputColumnNames.RECORD_ID)
    assert expected_results == extracted_row_values


def test_score_flat_results_by_frequency_sums_all_rows_correctly(spark_session):
    classified_rows = [
        (10, 1, 0, 0, 0, 0, 0, 0, 0, 0),
        (10, 1, 0, 0, 0, 0, 0, 0, 0, 0),
        (10, 0, 1, 0, 0, 0, 0, 0, 0, 0),
        (20, 0, 1, 0, 0, 0, 0, 0, 0, 0),
        (20, 0, 1, 0, 0, 0, 0, 0, 0, 0),
        (30, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    ]
    classified_rows_df = spark_session.createDataFrame(classified_rows, flat_classified_results_schema())
    results_df = score_flat_results_by_frequency(classified_rows_df)
    expected_results = [
        [10, 2, 1, 0, 0, 0, 0, 0, 0, 0],
        [20, 0, 2, 0, 0, 0, 0, 0, 0, 0],
        [30, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    ]
    extracted_row_values = extract_rows_for_col(results_df, all_expected_subcategory_column_names(),
                                                InputColumnNames.RECORD_ID)
    assert expected_results == extracted_row_values


def flat_classified_results_schema():
    # record_id, auto_sales, education, insurance, financial_services, real_estate, jobs, legal, home_services, other
    csv_schema = StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), False),
         StructField(ClassificationCategoryAbbreviations.AUTO_SALES, LongType(), False),
         StructField(ClassificationCategoryAbbreviations.EDUCATION, LongType(), False),
         StructField(ClassificationCategoryAbbreviations.INSURANCE, LongType(), False),
         StructField(ClassificationCategoryAbbreviations.FINANCIAL_SERVICES, LongType(), False),
         StructField(ClassificationCategoryAbbreviations.REAL_ESTATE, LongType(), False),
         StructField(ClassificationCategoryAbbreviations.JOBS, LongType(), False),
         StructField(ClassificationCategoryAbbreviations.LEGAL, LongType(), False),
         StructField(ClassificationCategoryAbbreviations.HOME_SERVICES, LongType(), False),
         StructField(ClassificationCategoryAbbreviations.OTHER, LongType(), False),
         ])
    return csv_schema


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
    col_names = [ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY, ClassificationSubcategory.CLASSIF_CATEGORY_KEY,
                 ClassificationSubcategory.SUBCATEGORY_CD,
                 ClassificationSubcategory.SUBCATEGORY_DISPLAY_NM, ClassificationSubcategory.INSERT_TS]
    return spark_session.createDataFrame(raw_hash_rows, col_names)


def scoring_input_schema():
    # record_id, input_id, classif_subcategory_key
    csv_schema = StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), False),
         StructField(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY, StringType(), True)])
    return csv_schema


def extract_rows_for_col(data_frame, col_names, order_by_column):
    # list comprehension is only way I can think of to make this easy
    # get Row objects and translate to Dict type
    rows_as_dicts = [i.asDict() for i in data_frame.select(col_names).orderBy(order_by_column).collect()]

    # from Dict get values in same order as column name COLUMN order
    list_values = [[row_dict.get(col_name) for col_name in col_names] for row_dict in rows_as_dicts]
    return list_values


def all_expected_subcategory_column_names():
    col_names = [InputColumnNames.RECORD_ID, ClassificationCategoryAbbreviations.AUTO_SALES,
                 ClassificationCategoryAbbreviations.EDUCATION,
                 ClassificationCategoryAbbreviations.INSURANCE, ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
                 ClassificationCategoryAbbreviations.REAL_ESTATE, ClassificationCategoryAbbreviations.JOBS,
                 ClassificationCategoryAbbreviations.LEGAL, ClassificationCategoryAbbreviations.HOME_SERVICES,
                 ClassificationCategoryAbbreviations.OTHER]
    return col_names
