from shared.utilities import ClassificationSubcategory, InputColumnNames


def score_file(classification_subcategories_df, classified_inputs_df):
    """
    Scores a classification DataFrame, currently using the frequency at which they appear.
    :param classification_subcategories_df: The subcategories table as a DataFrame
    :param classified_inputs_df: A DataFrame consisting of classified identifiers
    :return: A DataFrame of scored classifications.
    """
    subcategories_flat_df = flatten_subcategories(classification_subcategories_df)

    # JOIN on classif_subcategory_key and join in on flatted results.
    score_join = (classified_inputs_df.classif_subcategory_key == subcategories_flat_df.classif_subcategory_key)
    return classified_inputs_df.join(subcategories_flat_df, score_join, "left_outer") \
        .drop(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY,
              ClassificationSubcategory.CLASSIF_CATEGORY_KEY,
              ClassificationSubcategory.INSERT_TS,
              InputColumnNames.INPUT_ID) \
        .fillna(0)


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
