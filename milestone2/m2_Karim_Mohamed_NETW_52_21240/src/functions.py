import pandas as pd
import numpy as np

def rename_columns(df):
    df.columns = df.columns.str.lower()
    df.columns = [col.replace(' ', '_') for col in df.columns]
    return df

def set_index(df):
    df = df.set_index("loan_id")
    return df

def standardize_column_values(df):
    column = 'type'
    replacement = {
            r'.*individual.*': 'Individual',
            r'.*joint.*': 'Joint',
            r'direct_pay': 'Direct Pay'
            }
    df[column] = df[column].str.lower().replace(replacement, regex=True)
    column2 = 'home_ownership'
    df[column2] = df[column2].str.capitalize()
    return df

def impute_missing_data(df):
    df['int_rate'] = df['int_rate'].fillna(value=df['int_rate'].median())
    df = df.dropna(subset=['description'])
    return df
    
def impute_emp_title(row, df):
    if pd.isnull(row['emp_title']):
        nearest_rows = df[df['emp_title'].notnull()]
        if nearest_rows.empty:
            return 'Unknown'

        differences = (nearest_rows['annual_inc'] - row['annual_inc']).abs()
        nearest_index = differences.idxmin()
        return df.loc[nearest_index, 'emp_title']
    else:
        return row['emp_title']

def impute_emp_length(row, mode_emp_length):
    #def calculate_mode(group):
    #    if len(group) == 1:
    #        return group.iloc[0]
    #    else:
    #        return group.mode()[0] if not group.mode().empty else np.nan

    #mode_emp_length = df.groupby('emp_title')['emp_length'].agg(calculate_mode)
    if pd.isnull(row['emp_length']):
        emp_title = row['emp_title']
        if emp_title in mode_emp_length and pd.notnull(mode_emp_length[emp_title]):
            return mode_emp_length[emp_title]
        else:
            return np.nan
    else:
        return row['emp_length']

def handle_outliers(df):
    columns_to_cap = ['annual_inc', 'avg_cur_bal', 'tot_cur_bal']
    columns_to_replace = ['loan_amount', 'funded_amount', 'int_rate']
    for col in columns_to_cap:
        upper_limit = df[col].quantile(0.9)
        df.loc[:, col] = df[col].apply(lambda x: upper_limit if x > upper_limit else x)
    for col in columns_to_replace:
        median_value = df[col].median()
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        cut_off = IQR * 1.5
        lower = Q1 - cut_off
        upper = Q3 + cut_off
        df.loc[:, col] = df[col].apply(lambda x: median_value if x < lower or x > upper else x)
        return df

def add_month_column(df):
    #df.loc[:, 'issue_date'] = pd.to_datetime(df['issue_date'])
    print(df['issue_date'].dtype)
    df['issue_date'] = pd.to_datetime(df['issue_date'])
    print(df['issue_date'].dtype)
    df.loc[:, 'month_number'] = df['issue_date'].dt.month
    return df

def add_salary_column(df):
    df.loc[:, 'salary_can_cover'] = df['annual_inc'] >= df['loan_amount']
    return df

def add_letter_column(df):
    def letter_grade(grade):
        if 1 <= grade <= 5:
            return 'A'
        elif 6 <= grade <= 10:
            return 'B'
        elif 11 <= grade <= 15:
            return 'C'
        elif 16 <= grade <= 20:
            return 'D'
        elif 21 <= grade <= 25:
            return 'E'
        elif 26 <= grade <= 30:
            return 'F'
        elif 31 <= grade <= 35:
            return 'G'
        else:
            return None

    df.loc[:, 'letter_grade'] = df['grade'].apply(letter_grade)
    return df

def add_installment_column(df):
    df['term'] = df['term'].str.extract(r'(\d+)').astype(int)
    df.loc[:, 'monthly_int_rate'] = df['int_rate'] / 12
    df.loc[:, 'installment_per_month'] = df['loan_amount'] * df['monthly_int_rate'] * (1 + df['monthly_int_rate']) ** df['term'] / ((1 + df['monthly_int_rate']) ** df['term'] - 1)
    df = df.drop(columns=['monthly_int_rate'])
    df['installment_per_month'] = df['installment_per_month'].astype(float)
    return df

from sklearn.preprocessing import LabelEncoder

def label_encoding(df):
    label_encode_cols = ['emp_title', 'emp_length', 'zip_code', 'addr_state', 'loan_status', 'state', 'purpose', 'letter_grade']
    label_encoder = LabelEncoder()
    for col in label_encode_cols:
        df[col] = label_encoder.fit_transform(df[col])
    return df
        
def one_hot_encoding(df):
    one_hot_encode_cols = ['home_ownership', 'verification_status', 'type']
    df = pd.get_dummies(df, columns=one_hot_encode_cols, drop_first=True)
    return df

from scipy.stats import boxcox

def boxcox_normalization(df):
    lambdas_dict = {}
    columns = ['annual_inc', 'avg_cur_bal', 'loan_amount', 'funded_amount', 'tot_cur_bal', 'installment_per_month']
    for col in columns:
        positive_values = df[col] > 0
        positive_data = df.loc[positive_values, col]
        df.loc[positive_values, col], lambda_value = boxcox(positive_data)
        lambdas_dict[col] = lambda_value
    return df


def lookup_table(original_df, imputed_df, encoded_df):
    imputed_columns = ['int_rate', 'emp_title', 'emp_length', 'annual_inc', 'avg_cur_bal', 'tot_cur_bal', 'loan_amount', 'funded_amount']
    encoded_columns = ['emp_title', 'emp_length', 'zip_code', 'addr_state', 'loan_status', 'state', 'purpose', 'letter_grade', 'home_ownership', 'verification_status', 'type']
    lookup_data = []
    original_df = original_df.reset_index(drop=False)
    imputed_df = imputed_df.reset_index(drop=False)
    encoded_df = encoded_df.reset_index(drop=False)
    for col in imputed_columns:
        if col not in original_df.columns or col not in imputed_df.columns:
            print(f"column '{col}' not found, skip")
            continue
        mean_value = imputed_df[col].mean() if pd.api.types.is_numeric_dtype(imputed_df[col]) else None
        mask = original_df[col].isnull() & imputed_df[col].notnull()
        if mask.any():
            imputed_values = imputed_df[col][mask]
            for imputed_value in imputed_values:
                imputed_str = f"{imputed_value} (mean)" if mean_value is not None and imputed_value == mean_value else imputed_value
                lookup_data.append({'Column': col, 'Original': 'missing', 'Imputed': imputed_str})
        changed_mask = original_df[col] != imputed_df[col]
        if changed_mask.any():
            for original_value, imputed_value in zip(original_df[col][changed_mask], imputed_df[col][changed_mask]):
                imputed_str = f"{imputed_value} (mean)" if mean_value is not None and imputed_value == mean_value else imputed_value
                lookup_data.append({'Column': col, 'Original': original_value, 'Imputed': imputed_str})

    for col in encoded_columns:
        if col not in encoded_df.columns:
            print(f"column '{col}' not found in encoded_df, skip")
            continue
        original_values = original_df[col].unique() if col in original_df.columns else []
        encoded_values = encoded_df[col].unique() if col in encoded_df.columns else []
        for original in original_values:
            encoded = encoded_values[0] if len(encoded_values) > 0 else None
            lookup_data.append({'Column': col, 'Original': original, 'Imputed': encoded})

    lookup_df = pd.DataFrame(lookup_data)
    return lookup_df

def drop_column(df):
    df = df.drop('annual_inc_joint', axis=1)
    df = df.drop('customer_id', axis=1)
    return df

