import re
import sys

import numpy as np
import pandas as pd

import pdfplumber
from pathlib import Path


import ib_result_handler_summary as data_handler_sum
import ib_result_handler_subject as data_handler_sub
#import ib_result_handler_summary 
#import ib_result_handler_subject 


str_path_subject        = 'exam_results_subject_.csv'
#str_path_student       = 'exam_results.csv'
str_path_student        = 'raw_exam_results.csv'
str_path_output_csv     = 'exam_results_agg.csv'
str_path_output_excel   = 'exam_results_agg.xlsx'

lst_header_id = ['session_number', 'personal_code', 'subject']
# Determine common keys (example: candidate, name, birth_date)
lst_common_keys_formatted = ['session_number', 'personal_code', 'name', 'birth_date']
dict_subject_attr = {
    ''                      :   '',
    '(EXTENDED ESSAY)'      :   'ee',
    '(THEORY OF KNOWLEDGE)' :   'tk'
}

# --- Define your merge steps in a list of dictionaries ---
merge_steps = [
    {
        'merge_file_substring'  : '',
        'index_cols'            : ['session_number', 'personal_code', 'subject'],
        'merge_cols'            : ['pg', 'scaled_total'],
        'rename_map'            : None
    },
    {
        'merge_file_substring'  : '(EXTENDED ESSAY)',
        'index_cols'            : ['session_number', 'personal_code', 'subject'],
        'merge_cols'            : ['pg'],
        'rename_map'            : {'pg': 'ee_pg'}
    },
    {
        'merge_file_substring'  : '(THEORY OF KNOWLEDGE)',
        'index_cols'            : ['session_number', 'personal_code', 'subject'],
        'merge_cols'            : ['pg'],
        'rename_map'            : {'pg': 'tk_pg'}
    }
]



#==================================================================================================
def merge_ib_results( df_main, merge_file_substring, 
                        index_cols,merge_cols, rename_map=None, how='left'
                        ):
    """
    Generic function to merge IB exam results with a subject or EE file.
    df_main: DataFrame to merge into (can be a DataFrame or a CSV path).
    merge_file_substring: CSV path to merge from.
    index_cols: list of columns to join on.
    merge_cols: list of columns to merge from merge_file_substring.
    rename_map: dict for renaming columns from merge_file_substring before merging.
    Returns: merged DataFrame.
    """
    if isinstance(df_main, str):
        df_main = pd.read_csv(df_main)

    # ==================================================================================================
    # Insert subject type before extension
    str_filepath_tmp = Path(str_path_subject)
    str_filepath_tmp = str_filepath_tmp.with_name(str_filepath_tmp.stem + merge_file_substring + str_filepath_tmp.suffix)
    #print(str_filepath_tmp)
    #sys.exit()
    # ==================================================================================================
    df_merge = pd.read_csv(str_filepath_tmp)
    cols_to_use = index_cols + merge_cols
    df_merge = df_merge[cols_to_use]
    if rename_map:
        df_merge = df_merge.rename(columns=rename_map)
    df_merged = pd.merge(df_main, df_merge, on=index_cols, how=how, suffixes=('', '_new'))
    # If any columns are meant to update existing ones, do so
    for col in merge_cols:
        target_col = rename_map[col] if rename_map and col in rename_map else col
        if target_col in df_main.columns and f"{target_col}_new" in df_merged.columns:
            df_merged[target_col] = df_merged[f"{target_col}_new"].combine_first(df_merged[target_col])
            df_merged.drop(columns=[f"{target_col}_new"], inplace=True)
    return df_merged
#==================================================================================================



#==================================================================================================

def main():
    print('start')
    # --- Run the merges in a loop ---
    df_main = pd.read_csv(str_path_student)
    for step in merge_steps:
        df_main = merge_ib_results(
            df_main=df_main,
            merge_file_substring=step['merge_file_substring'],
            index_cols=step['index_cols'],
            merge_cols=step['merge_cols'],
            rename_map=step['rename_map']
        )

    # --- Save or inspect the final merged DataFrame ---
    #df_main = df_main.reset_index()
    
    df_main.to_csv('raw_' + str_path_output_csv, index=False)
    print(df_main.head())
    # ===================================================
    df_sub      = data_handler_sum.reformat_results_sub(df_main)
    df_overall  = data_handler_sum.reformat_results_overall(df_main)
    merged_df = pd.merge(df_sub, df_overall, on=lst_common_keys_formatted, how='outer')

    merged_df.to_csv(str_path_output_csv)
    merged_df.to_excel(str_path_output_excel)
    print(merged_df.head())

if __name__ == '__main__':
    main()
#==================================================================================================
