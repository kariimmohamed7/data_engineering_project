import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from functions import *

db_url = "postgresql://root:root@pgdatabase:5432/fintech"
clean_data_path = 'data/fintech_data_NETW_P2_52_21240_clean.csv'
raw_data_path = 'data/fintech_data_23_52_21240.csv'
lookup_table_path = 'data/lookup_table.csv'

def connect_to_db():
    engine = create_engine(db_url)
    print("created engine")
    return engine

def upload_to_db(engine, df, table_name):
    try:
        df.to_sql(table_name, engine, if_exists='fail', index=False)
        print('csv file uploaded to database')
    except ValueError as e:
        print("table already exists, Error: ", e)

if __name__ == "__main__":
    if not os.path.exists(clean_data_path):
        print("did not find clean dataset, operating on raw")
        df = pd.read_csv(raw_data_path)
        imputed_df = rename_columns(df)
        imputed_df = set_index(imputed_df)
        imputed_df = standardize_column_values(imputed_df)
        imputed_df = impute_missing_data(imputed_df)
        imputed_df.loc[:, 'emp_title'] = imputed_df.apply(lambda row: impute_emp_title(row, imputed_df), axis=1)
        print("imputed emp_title successfully")
        mode_emp_length = imputed_df.groupby('emp_title')['emp_length'].agg(lambda x: x.mode()[0] if not x.mode().empty else np.nan).to_dict()
        imputed_df.loc[:, 'emp_length'] = imputed_df.apply(lambda row: impute_emp_length(row, mode_emp_length), axis=1)
        print("imputed emp_length successfully")
        imputed_df = handle_outliers(imputed_df)
        imputed_df = add_month_column(imputed_df)
        imputed_df = add_salary_column(imputed_df)
        imputed_df = add_letter_column(imputed_df)
        imputed_df = add_installment_column(imputed_df)
        print("finished imputing")
        encoded_df = label_encoding(imputed_df)
        encoded_df = one_hot_encoding(encoded_df)
        print("finished encoding")
        clean_data = boxcox_normalization(encoded_df)
        clean_data = drop_column(clean_data)
        print("finalized clean dataset")
        df_test = df.copy()
        df_test = df_test.dropna(subset=['description'])
        lookup = lookup_table(df_test, imputed_df, encoded_df)
        print("finished lookup table")
        clean_data.to_csv(clean_data_path, index=False)
        print("saved clean data to csv")
        lookup.to_csv(lookup_table_path, index=False)
        print("saved lookup table to csv")
    else:
        print("found clean dataset and lookup table")
        clean_data = pd.read_csv(clean_data_path)
        lookup = pd.read_csv(lookup_table_path)

    engine = connect_to_db()
    upload_to_db(engine, clean_data, "cleaned_data")
    upload_to_db(engine, lookup, "lookup_table")

