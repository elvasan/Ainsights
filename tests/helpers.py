def extract_rows_for_col(data_frame, col_name):
    return [i[col_name] for i in data_frame.select(col_name).collect()]


def extract_rows_for_col_with_order(data_frame, col_names, order_by_column):
    # list comprehension is only way I can think of to make this easy
    # get Row objects and translate to Dict type
    rows_as_dicts = [i.asDict() for i in data_frame.select(col_names).orderBy(order_by_column).collect()]

    # from Dict get values in same order as column name COLUMN order
    list_values = [[row_dict.get(col_name) for col_name in col_names] for row_dict in rows_as_dicts]
    return list_values
