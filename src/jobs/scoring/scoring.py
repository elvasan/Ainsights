from shared.utilities import ClassificationSubcategory, InputColumnNames
from pyspark.sql.functions import sum  # pylint:disable=redefined-builtin


def score_file(classification_subcategories_df, classified_inputs_df):
    """
    Scores a classification DataFrame, currently using the frequency at which they appear.
    :param classification_subcategories_df: The subcategories table as a DataFrame
    :param classified_inputs_df: A DataFrame consisting of classified identifiers
    :return: A DataFrame of scored classifications.
    """
    subcategories_flat_df = flatten_subcategories(classification_subcategories_df)

    raw_scores_df = join_classified_inputs_to_subcategories(subcategories_flat_df, classified_inputs_df)
    scored_by_record_id_df = score_flat_results_by_frequency(raw_scores_df)
    return scored_by_record_id_df


def flatten_subcategories(classification_subcategories_df):
    """
    Will flatten out all subcategories for a given classification DataFrame and pivot them to be headers.
    :param classification_subcategories_df:
    :return: A DataFrame consisting of subcategories as headers to join in on the classified DataFrame
    """
    return classification_subcategories_df \
        .groupBy(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY) \
        .pivot(ClassificationSubcategory.SUBCATEGORY_CD) \
        .agg({ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY: "count"}) \
        .fillna(0)


def join_classified_inputs_to_subcategories(subcategories_flat_df, classified_inputs_df):
    # subcategories_flat_df is all classification subcategories represented as rows so we can join to them
    # classif_subcategory_key   auto_sales  education   insurance, etc..
    # 1                         1           0           0
    # 2                         0           1           0
    # 3                         0           0           1
    #
    # classified_inputs_df is raw output from classification
    # record_id                 classif_subcategory_key
    # 100                       1
    # 100                       2
    # 100                       1
    # 101                       1
    #
    # Returned Results would look like
    # record_id                 auto_sales  education   insurance   etc.
    # 100                       1           0           0
    # 100                       0           1           0
    # 100                       1           0           0
    # 101                       1           0           0
    # JOIN on classif_subcategory_key and join in on flatted results.
    score_join = (classified_inputs_df.classif_subcategory_key == subcategories_flat_df.classif_subcategory_key)
    return classified_inputs_df.join(subcategories_flat_df, score_join, "left_outer") \
        .drop(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY) \
        .fillna(0)


def score_flat_results_by_frequency(classified_inputs_df):
    # Formula takes a raw input set and returns 'counted' values for each classification
    # classified_inputs_df
    # record_id                 auto_sales  education   insurance   etc.
    # 100                       1           0           0
    # 100                       0           1           0
    # 100                       1           0           0
    # 101                       1           0           0
    #
    # outputs
    # record_id                 auto_sales  education   insurance   etc.
    # 100                       2           1           0
    # 101                       1           0           0

    # get list of existing column names (use copy because will manipulate column list)
    scored_columns = classified_inputs_df.columns.copy()
    scored_columns.remove(InputColumnNames.RECORD_ID)  # don't want this value to be aggregated

    # select columns alphabetically (with record_id first)
    selected_columns = scored_columns.copy()
    selected_columns.sort()
    selected_columns.insert(0, InputColumnNames.RECORD_ID)

    # sum each column and rename to original column name
    # e.g. sum(auto_sales).alias(auto_sales)
    sum_aggregate_expressions = [sum(col_name).alias("{0}".format(col_name)) for col_name in scored_columns]
    # group by record_id using aggregate function
    classified_inputs_totaled_df = classified_inputs_df.groupBy(InputColumnNames.RECORD_ID)\
        .agg(*sum_aggregate_expressions)
    return classified_inputs_totaled_df.select(selected_columns).orderBy(InputColumnNames.RECORD_ID)
