import pytest
from pyspark.sql.types import StructField, StructType, StringType, LongType, BooleanType

from jobs.init import config
from jobs.scoring import scoring
from shared.constants import ClassificationSubcategory, InputColumnNames, ClassificationCategoryAbbreviations, \
    ThresholdValues, ConfigurationOptions
from tests.helpers import extract_rows_for_col_with_order

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_flatten_subcategories_returns_rows_with_key_and_abbreviation(spark_session):
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = scoring.flatten_subcategories(subcategory_df)
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
    expected_cols = [ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY,
                     ClassificationCategoryAbbreviations.AUTO_SALES,
                     ClassificationCategoryAbbreviations.EDUCATION,
                     ClassificationCategoryAbbreviations.INSURANCE,
                     ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
                     ClassificationCategoryAbbreviations.REAL_ESTATE,
                     ClassificationCategoryAbbreviations.JOBS,
                     ClassificationCategoryAbbreviations.LEGAL,
                     ClassificationCategoryAbbreviations.HOME_SERVICES,
                     ClassificationCategoryAbbreviations.OTHER]
    extracted_row_values = extract_rows_for_col_with_order(result_df,
                                                           expected_cols,
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
        (90, 9)
    ]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = scoring.score_file(subcategory_df, input_df)
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
    extracted_row_values = extract_rows_for_col_with_order(result_df,
                                                           all_expected_subcategory_column_names(),
                                                           InputColumnNames.RECORD_ID)
    assert expected_results == extracted_row_values


def test_score_file_transforms_with_multiple_values_for_each_classification_frequency(spark_session):
    input_rows = [
        (10, 1), (10, 1), (10, 2), (10, 1), (10, 6),
        (20, 2), (20, 3), (20, 4), (20, 5), (20, 5)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = scoring.score_file(subcategory_df, input_df)
    expected_results = [
        [10, 3, 1, 0, 0, 0, 1, 0, 0, 0],
        [20, 0, 1, 1, 1, 2, 0, 0, 0, 0]
    ]
    extracted_row_values = extract_rows_for_col_with_order(result_df,
                                                           all_expected_subcategory_column_names(),
                                                           InputColumnNames.RECORD_ID)
    assert expected_results == extracted_row_values


def test_score_file_returns_category_abbrev_column_values(spark_session):
    input_rows = [(10, 1)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = scoring.score_file(subcategory_df, input_df)
    assert sorted(result_df.schema.names) == sorted(all_expected_subcategory_column_names())


def test_score_file_with_empty_input_file(spark_session):
    input_rows = []
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    subcategory_df = define_classification_subcategory_df(spark_session)
    result_df = scoring.score_file(subcategory_df, input_df)
    assert result_df.count() == 0


def test_join_classified_inputs_to_subcategories_returns_flat_results(spark_session):
    input_rows = [
        (10, 1),
        (10, 1),
        (20, 3)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    raw_subcategory_df = define_classification_subcategory_df(spark_session)
    subcategory_df = scoring.flatten_subcategories(raw_subcategory_df)
    result_df = scoring.join_classified_inputs_to_subcategories(subcategory_df, input_df)
    expected_results = [
        [10, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [10, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [20, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    ]
    extracted_row_values = extract_rows_for_col_with_order(result_df,
                                                           all_expected_subcategory_column_names(),
                                                           InputColumnNames.RECORD_ID)
    assert expected_results == extracted_row_values


def test_join_classified_inputs_to_subcategories_returns_zeros_for_no_classification_found(spark_session):
    input_rows = [
        (10, 1),
        (20, None),  # no classification found
        (30, 3)]
    input_df = spark_session.createDataFrame(input_rows, scoring_input_schema())
    raw_subcategory_df = define_classification_subcategory_df(spark_session)
    subcategory_df = scoring.flatten_subcategories(raw_subcategory_df)
    result_df = scoring.join_classified_inputs_to_subcategories(subcategory_df, input_df)
    expected_results = [
        [10, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [20, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # row should be all zeros
        [30, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    ]
    extracted_row_values = extract_rows_for_col_with_order(result_df,
                                                           all_expected_subcategory_column_names(),
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
    results_df = scoring.score_flat_results_by_frequency(classified_rows_df)
    expected_results = [
        [10, 2, 1, 0, 0, 0, 0, 0, 0, 0],
        [20, 0, 2, 0, 0, 0, 0, 0, 0, 0],
        [30, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    ]
    extracted_row_values = extract_rows_for_col_with_order(results_df,
                                                           all_expected_subcategory_column_names(),
                                                           InputColumnNames.RECORD_ID)
    assert expected_results == extracted_row_values


def test_apply_thresholds_to_scored_df_with_varying_scores_returns_multiple_in_market_values(spark_session):
    # Define a scoring DataFrame
    scored_values = [(100, 0, 1, 4, 4, 0, 0, 5, 2, 0)]
    scoring_df = spark_session.createDataFrame(scored_values, flat_classified_results_schema())
    # Define an app config dataframe
    config_values = [(ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.AUTO_SALES, 3),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.EDUCATION, 2),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.INSURANCE, 4),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
                      3),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.REAL_ESTATE, 4),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.JOBS, 5),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.LEGAL, 3),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.HOME_SERVICES, 2),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.OTHER, 2)]
    app_config_df = spark_session.createDataFrame(config_values, config.configuration_schema())
    external_result_df = scoring.apply_thresholds_to_scored_df(scoring_df, app_config_df)
    results = extract_rows_for_col_with_order(external_result_df, all_expected_subcategory_column_names(),
                                              InputColumnNames.RECORD_ID)
    assert results == [[100,
                        ThresholdValues.NOT_SEEN,
                        ThresholdValues.EARLY_JOURNEY,
                        ThresholdValues.EARLY_JOURNEY,
                        ThresholdValues.LATE_JOURNEY,
                        ThresholdValues.NOT_SEEN,
                        ThresholdValues.NOT_SEEN,
                        ThresholdValues.LATE_JOURNEY,
                        ThresholdValues.EARLY_JOURNEY,
                        ThresholdValues.NOT_SEEN]]


def test_apply_thresholds_to_scored_df_with_multiple_rows_returns_values_for_all(spark_session):
    # Define a scoring DataFrame
    scored_values = [(100, 0, 1, 4, 4, 0, 0, 5, 2, 0),
                     (101, 0, 1, 4, 4, 0, 0, 5, 2, 0),
                     (102, 0, 1, 4, 4, 0, 0, 5, 2, 0)]
    scoring_df = spark_session.createDataFrame(scored_values, flat_classified_results_schema())
    # Define an app config dataframe
    config_values = [(ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.AUTO_SALES, 3),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.EDUCATION, 2),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.INSURANCE, 4),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
                      3),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.REAL_ESTATE, 4),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.JOBS, 5),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.LEGAL, 3),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.HOME_SERVICES, 2),
                     (ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.OTHER, 2)]
    app_config_df = spark_session.createDataFrame(config_values, config.configuration_schema())
    external_result_df = scoring.apply_thresholds_to_scored_df(scoring_df, app_config_df)
    results = extract_rows_for_col_with_order(external_result_df, all_expected_subcategory_column_names(),
                                              InputColumnNames.RECORD_ID)
    assert results == [
        [100,
         ThresholdValues.NOT_SEEN,
         ThresholdValues.EARLY_JOURNEY,
         ThresholdValues.EARLY_JOURNEY,
         ThresholdValues.LATE_JOURNEY,
         ThresholdValues.NOT_SEEN,
         ThresholdValues.NOT_SEEN,
         ThresholdValues.LATE_JOURNEY,
         ThresholdValues.EARLY_JOURNEY,
         ThresholdValues.NOT_SEEN
         ],
        [101,
         ThresholdValues.NOT_SEEN,
         ThresholdValues.EARLY_JOURNEY,
         ThresholdValues.EARLY_JOURNEY,
         ThresholdValues.LATE_JOURNEY,
         ThresholdValues.NOT_SEEN,
         ThresholdValues.NOT_SEEN,
         ThresholdValues.LATE_JOURNEY,
         ThresholdValues.EARLY_JOURNEY,
         ThresholdValues.NOT_SEEN
         ],
        [102,
         ThresholdValues.NOT_SEEN,
         ThresholdValues.EARLY_JOURNEY,
         ThresholdValues.EARLY_JOURNEY,
         ThresholdValues.LATE_JOURNEY,
         ThresholdValues.NOT_SEEN,
         ThresholdValues.NOT_SEEN,
         ThresholdValues.LATE_JOURNEY,
         ThresholdValues.EARLY_JOURNEY,
         ThresholdValues.NOT_SEEN
         ]
    ]


def test_apply_thresholds_to_scored_df_with_auto_value_greater_than_threshold_returns_in_market_h(spark_session):
    # Define a scoring DataFrame
    scored_values = [(100, 5)]
    scoring_schema = StructType([StructField(InputColumnNames.RECORD_ID, LongType()),
                                 StructField(ClassificationCategoryAbbreviations.AUTO_SALES, LongType())])
    scoring_df = spark_session.createDataFrame(scored_values, scoring_schema)
    # Define an app config dataframe
    config_values = [(ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.AUTO_SALES, 3)]
    app_config_df = spark_session.createDataFrame(config_values, config.configuration_schema())
    external_result_df = scoring.apply_thresholds_to_scored_df(scoring_df, app_config_df)
    results = extract_rows_for_col_with_order(external_result_df,
                                              [InputColumnNames.RECORD_ID,
                                               ClassificationCategoryAbbreviations.AUTO_SALES],
                                              InputColumnNames.RECORD_ID)
    assert results == [[100, ThresholdValues.LATE_JOURNEY]]


def test_apply_thresholds_to_scored_df_with_auto_value_equal_to_threshold_returns_in_market(spark_session):
    # Define a scoring DataFrame
    scored_values = [(100, 3)]
    scoring_schema = StructType([StructField(InputColumnNames.RECORD_ID, LongType()),
                                 StructField(ClassificationCategoryAbbreviations.AUTO_SALES, LongType())])
    scoring_df = spark_session.createDataFrame(scored_values, scoring_schema)
    # Define an app config dataframe
    config_values = [(ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.AUTO_SALES, 3)]
    app_config_df = spark_session.createDataFrame(config_values, config.configuration_schema())
    external_result_df = scoring.apply_thresholds_to_scored_df(scoring_df, app_config_df)
    results = extract_rows_for_col_with_order(external_result_df,
                                              [InputColumnNames.RECORD_ID,
                                               ClassificationCategoryAbbreviations.AUTO_SALES],
                                              InputColumnNames.RECORD_ID)
    assert results == [[100, ThresholdValues.EARLY_JOURNEY]]


def test_apply_thresholds_to_scored_df_with_auto_value_less_than_threshold_returns_in_market(spark_session):
    # Define a scoring DataFrame
    scored_values = [(100, 1)]
    scoring_schema = StructType([StructField(InputColumnNames.RECORD_ID, LongType()),
                                 StructField(ClassificationCategoryAbbreviations.AUTO_SALES, LongType())])
    scoring_df = spark_session.createDataFrame(scored_values, scoring_schema)
    # Define an app config dataframe
    config_values = [(ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.AUTO_SALES, 3)]
    app_config_df = spark_session.createDataFrame(config_values, config.configuration_schema())
    external_result_df = scoring.apply_thresholds_to_scored_df(scoring_df, app_config_df)
    results = extract_rows_for_col_with_order(external_result_df,
                                              [InputColumnNames.RECORD_ID,
                                               ClassificationCategoryAbbreviations.AUTO_SALES],
                                              InputColumnNames.RECORD_ID)
    assert results == [[100, ThresholdValues.EARLY_JOURNEY]]


def test_apply_thresholds_to_scored_df_with_auto_value_zero_returns_not_seen(spark_session):
    # Define a scoring DataFrame
    scored_values = [(100, 0)]
    scoring_schema = StructType([StructField(InputColumnNames.RECORD_ID, LongType()),
                                 StructField(ClassificationCategoryAbbreviations.AUTO_SALES, LongType())])
    scoring_df = spark_session.createDataFrame(scored_values, scoring_schema)
    # Define an app config dataframe
    config_values = [(ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.AUTO_SALES, 3)]
    app_config_df = spark_session.createDataFrame(config_values, config.configuration_schema())
    external_result_df = scoring.apply_thresholds_to_scored_df(scoring_df, app_config_df)
    results = extract_rows_for_col_with_order(external_result_df,
                                              [InputColumnNames.RECORD_ID,
                                               ClassificationCategoryAbbreviations.AUTO_SALES],
                                              InputColumnNames.RECORD_ID)
    assert results == [[100, ThresholdValues.NOT_SEEN]]


def test_apply_thresholds_to_scored_df_should_ignore_extra_columns(spark_session):
    # Define a scoring DataFrame
    scored_values = [(100, 0, False, 3, 'Hello World')]
    scoring_schema = StructType([StructField(InputColumnNames.RECORD_ID, LongType()),
                                 StructField(ClassificationCategoryAbbreviations.AUTO_SALES, LongType()),
                                 StructField('has_seen', BooleanType()),
                                 StructField('a_number', LongType()),
                                 StructField('a_string', StringType())])
    scoring_df = spark_session.createDataFrame(scored_values, scoring_schema)
    # Define an app config dataframe
    config_values = [(ConfigurationOptions.FREQUENCY_THRESHOLD, ClassificationCategoryAbbreviations.AUTO_SALES, 3)]
    app_config_df = spark_session.createDataFrame(config_values, config.configuration_schema())
    external_result_df = scoring.apply_thresholds_to_scored_df(scoring_df, app_config_df)
    results = extract_rows_for_col_with_order(external_result_df,
                                              external_result_df.columns,
                                              InputColumnNames.RECORD_ID)
    assert sorted(external_result_df.columns) == sorted(['record_id', 'auto_sales', 'has_seen', 'a_number', 'a_string'])
    assert results == [[100, ThresholdValues.NOT_SEEN, False, 3, 'Hello World']]


def flat_classified_results_schema():
    # record_id, auto_sales, education, insurance, financial_services, real_estate, jobs, legal, home_services, other
    return StructType(
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

    col_names = [ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY,
                 ClassificationSubcategory.CLASSIF_CATEGORY_KEY,
                 ClassificationSubcategory.SUBCATEGORY_CD,
                 ClassificationSubcategory.SUBCATEGORY_DISPLAY_NM,
                 ClassificationSubcategory.INSERT_TS]

    return spark_session.createDataFrame(raw_hash_rows, col_names)


def scoring_input_schema():
    # record_id, classif_subcategory_key
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), False),
         StructField(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY, StringType(), True)])


def all_expected_subcategory_column_names():
    return [InputColumnNames.RECORD_ID,
            ClassificationCategoryAbbreviations.AUTO_SALES,
            ClassificationCategoryAbbreviations.EDUCATION,
            ClassificationCategoryAbbreviations.INSURANCE,
            ClassificationCategoryAbbreviations.FINANCIAL_SERVICES,
            ClassificationCategoryAbbreviations.REAL_ESTATE,
            ClassificationCategoryAbbreviations.JOBS,
            ClassificationCategoryAbbreviations.LEGAL,
            ClassificationCategoryAbbreviations.HOME_SERVICES,
            ClassificationCategoryAbbreviations.OTHER]
