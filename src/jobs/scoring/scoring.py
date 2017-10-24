from shared.utilities import COL_NAME_INSERTED_TIMESTAMP, COL_NAME_CLASSIF_SUBCATEGORY_KEY, \
    COL_NAME_CLASSIF_CATEGORY_KEY, COL_NAME_INPUT_ID, COL_NAME_SUBCATEGORY_NAME


def score_file(classification_subcategories_df, classified_inputs_df):
    # flatten out all subcategories
    subcategories_flat_df = flatten_subcategories(classification_subcategories_df)

    # JOIN on classif_subcategory_key
    score_join = (classified_inputs_df.classif_subcategory_key == subcategories_flat_df.classif_subcategory_key)
    flat_results_df = classified_inputs_df.join(subcategories_flat_df, score_join, "left_outer")

    # remove columns not wanted and put 0 for any None columns
    formatted_results_df = flat_results_df.drop(COL_NAME_CLASSIF_SUBCATEGORY_KEY,
                                                COL_NAME_CLASSIF_CATEGORY_KEY,
                                                COL_NAME_INSERTED_TIMESTAMP,
                                                COL_NAME_INPUT_ID).fillna(0)
    return formatted_results_df


def flatten_subcategories(classification_subcategories_df):
    # flatten out all subcategories
    subcategories_flat_df = classification_subcategories_df.groupBy(COL_NAME_CLASSIF_SUBCATEGORY_KEY) \
        .pivot(COL_NAME_SUBCATEGORY_NAME).agg(
        {COL_NAME_CLASSIF_SUBCATEGORY_KEY: "count"}).fillna(0)
    return subcategories_flat_df
