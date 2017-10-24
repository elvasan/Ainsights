from shared.utilities import ClassificationColumnNames, InputColumnNames


def score_file(classification_subcategories_df, classified_inputs_df):
    # flatten out all subcategories
    subcategories_flat_df = flatten_subcategories(classification_subcategories_df)

    # JOIN on classif_subcategory_key and join in on flatted results.
    score_join = (classified_inputs_df.classif_subcategory_key == subcategories_flat_df.classif_subcategory_key)
    return classified_inputs_df.join(subcategories_flat_df, score_join, "left_outer") \
        .drop(ClassificationColumnNames.SUBCATEGORY_KEY,
              ClassificationColumnNames.CATEGORY_KEY,
              ClassificationColumnNames.INSERTED_TIMESTAMP,
              InputColumnNames.INPUT_ID) \
        .fillna(0)


def flatten_subcategories(classification_subcategories_df):
    # flatten out all subcategories
    return classification_subcategories_df \
        .groupBy(ClassificationColumnNames.SUBCATEGORY_KEY) \
        .pivot(ClassificationColumnNames.SUBCATEGORY_NAME) \
        .agg({ClassificationColumnNames.SUBCATEGORY_KEY: "count"}) \
        .fillna(0)
