import re
import sys

import numpy as np
import pandas as pd

import pdfplumber

# Path to the PDF file
PDF_PATH    = 'display_report.pdf'
# Output CSV path
CSV_PATH    = 'exam_results.csv'
XLSX_PATH   = 'exam_results.xlsx'


#============================================================================
dict_fields_to_extract = {
    "date_printed"  :   "Date printed:",
    "candidate"     :   "Candidate",
    "name"          :   "Name",
    "category"      :   "Category",
    "birth_date"    :   "Birth Date",
    "pt_ee_tok"     :   "EE/TOK points:",
    "pt_total"      :   "Total Points:",
    "result"        :   "Result:",
    #======================================
    "grade"         :   "Grade",
    "subject"       :   "Subject"
}

str_header_summary_student_grade_subjects   =  'Grade'
str_header_summary_student_grade_ee_tok     = 'EE/TOK points:'

lst_exclude_keyword_ee_tok = ['ee', 'tk']
#initial str for "grade" and "subject"
common_keys = ['session_number', 'personal_code', 'name', 'birth_date']
#============================================================================




def pasrse_field_single(str_search_keyword, text):
    pattern = rf"{re.escape(str_search_keyword)}\s*(.+)"
    match = re.search(pattern, text)

    m_res = (match.group(1).strip()) if match else None
    return m_res

def parse_page(text):
    """
    Parse the text of one PDF page and extract student exam results.
    Returns a dict of fields.
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    data = {}
    dict_record_lv_top = {}
    lst_record_lv_subject = []

    #Extract Single field data form the page
    for key, value in dict_fields_to_extract.items():
        dict_record_lv_top[key] = pasrse_field_single(value, text)

    pattern = ''.join(str_header_summary_student_grade_subjects) + r'\s*(.+)'
    #m_grade = re.search(pattern, text)
    m_grade = re.search(r'Grade\s*(.+)', text)
    if m_grade:
        # find its line index
        for i, l in enumerate(lines):
            if l.startswith(str_header_summary_student_grade_subjects):

                raw_lines = lines[i + 1:]
                for line in raw_lines:
                    if line.startswith(str_header_summary_student_grade_ee_tok) or not line.strip():
                        break
                    match = re.match(r'^(\S+)\s+(.+)', line.strip())
                    if match:
                        dict_tmp_reocrd = dict_record_lv_top.copy()
                        dict_tmp_reocrd['grade'] = match.group(1)
                        dict_tmp_reocrd['subject'] = match.group(2)    
                        lst_record_lv_subject = lst_record_lv_subject + [dict_tmp_reocrd]
                break
    
    # for item in lst_record_lv_subject:
    #     for key, value in item.items():
    #         print(f"{key}: {value}")
    #     print("=======")
    #     #print(f"Grade: {item['Grade']}, Subject: {item['Subject']}")
    # sys.exit(0)
    return lst_record_lv_subject


def extract_results(pdf_path):
    """
    Extract results for all pages in the given PDF.
    Returns a DataFrame.
    """
    records = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            rec = parse_page(text)
            #records.append(rec)
            records = records + rec

    # Normalize into DataFrame
    df = pd.json_normalize(records)

    return df

def reformat_results(df):
    # 1. Split 'subject' into session and subject name
    # Split the 'subject' column on ' - ' into two new columns: 'session' and 'subject'
    df[['session', 'subject']] = df['subject'].str.split(' - ', n=1, expand=True)
    df[['session', 'candidate']] = df['candidate'].str.split(' - ', n=1, expand=True)

    # 2. Extract core subject name and type (SL/HL/EE/TK)
    # Extract before and after 'SL', 'HL', or 'EE' using regex
    #df[['sub', 'uni_pg']] = df['subject'].str.extract(r'^(.*?)(SL|HL|EE.*)$')
    df[['subject_', 'sub']]                     = df['subject'].str.extract(r'^(.*?)(\b(?:SL|HL|EE|TK)\b.*)$')

    #df[['session_number', 'personal_code']]     = df['candidate'].str.extract(r'^(\d{6}) (\d{4})')
    df[['session_number', 'personal_code']]    = df['candidate'].str.extract(r'^(.+?)\s*(\([^)]+\))$')

    # Clean whitespace
    df['subject_'] = df['subject_'].str.strip()
    df['sub'] = df['sub'].str.strip()
    
    # 3. Assign grade components
    df['uni_pg'] = ""
    #df['sub'] = ""
    #df['uni_pg'] = ""
    df['PG'] = ""
    df['FG'] = df['grade']
    df['scaled_total'] = ""

    #==============================================================================================
    # map EE and TOK sub and FG to 
    for i in lst_exclude_keyword_ee_tok:
        # Join into regex pattern: 'ee|tk'
        #         r'^(.*?)\s+ee\b'
        pattern = r'^(.*?)\s' + '+'.join(i) + r'\b'
        df[i + '_sub'] = df['subject'].str.extract( pattern, flags=re.IGNORECASE)
        df[i + '_fg'] = np.where(
                                    df['subject'].str.contains( i , case=False, na=False),
                                    #df['subject'].str.extract( i , case=False, na=False),
                                    df['grade'],
                                    np.nan
                                )
        df[i + '_pg'] = ""
    #==============================================================================================

    return df

def reformat_results_sub(df):

    # Join into regex pattern: 'ee|tk'
    pattern = r'\b(' + '|'.join(lst_exclude_keyword_ee_tok) + r')\b'
    
    # Corrected pattern for case-insensitivity within the regex
    # re.IGNORECASE is embedded as (?i)
    pattern = r'(?i)\b(' + '|'.join(lst_exclude_keyword_ee_tok) + r')\b'
    
    # Filter rows where 'subject' does NOT contain any keyword
    #df = df[~df['subject'].str.contains(pattern, case=False, na=False, flags=re.IGNORECASE)]
    #df = df[~df['subject'].str.extract(pattern, case=False, na=False, flags=re.IGNORECASE)]

    # The fix: remove 'case' and 'flags' arguments from str.extract()
    # Use .notna() to filter out rows where no match was found (NaN)
    df = df[~df['subject'].str.extract(pattern, expand=False).notna()]

    # 4. Pivot table: group by student, columns by subject, values are grade fields
    pivot_table = pd.pivot_table(
      df,
      values    =   ['sub', 'uni_pg', 'PG', 'FG', 'scaled_total'],
      index     =   ['session_number', 'personal_code', 'name', 'birth_date'],
      columns   =   ['subject_'],
      aggfunc   =   'first'
    )

    # 5. Reorder column levels to [subject, field]
    pivot_table.columns = pivot_table.columns.reorder_levels([1, 0])
    pivot_table = pivot_table.sort_index(axis=1, level=0)

    # 6. Reset index for clean export
    #pivot_table = pivot_table.reset_index()

    #==============================================================================================
    # Get all unique subjects from the first level of columns
    subjects = pivot_table.columns.get_level_values(0).unique()
    # Build new column order
    new_columns = []
    for subject in subjects:
        for col in ['sub', 'uni_pg', 'PG', 'FG', 'scaled_total']:
            if (subject, col) in pivot_table.columns:
                new_columns.append((subject, col))

    # Reindex the columns to the new order
    pivot_table = pivot_table.reindex(columns=pd.MultiIndex.from_tuples(new_columns))
    #========================================================================================
    # 6. Reset index for clean export
    #pivot_table = pivot_table.reset_index()
    
    return pivot_table


def reformat_results_overall(df):
    '''
    ["date_printed","candidate","name","category","birth_date","pt_ee_tok","pt_total","result"]
    '''

    lst_ee_tok = ['ee_sub', 'ee_pg', 'ee_fg', 'tk_sub', 'tk_pg', 'tk_fg']

    # Select only the required columns
    df = df[['session_number', 'personal_code', 'name', 'birth_date', 
            'ee_sub', 'ee_pg', 'ee_fg', 'tk_sub', 'tk_pg', 'tk_fg',
            'pt_ee_tok', 'pt_total', 'result']]

    # If you want to group by these columns (for aggregation), for example to get the first record per group:
    df = df.groupby(['session_number', 'personal_code', 'name', 'birth_date'], as_index=True).first()
    print(df)
    # Create MultiIndex columns
    arrays = [
        ['pt_ee_tok', 'pt_ee_tok', 'ee', 'ee', 'ee',  'tk',  'tk', 'tk',    'pt_total', 'pt_total', 'result',                             'result'],
        ['PG',        'FG',        'sub','PG', 'FG',  'sub', 'PG', 'FG',    'PG',       'FG',       '1 = bilingual, 2 = pass, 0 = fail',      'FG']
    ]
    multi_cols = pd.MultiIndex.from_arrays(arrays, names=['', 'GradeType'])

    # Build conditional PG values for 'result'
    result_cal_value = np.select(
        [
            df['result'].str.contains('bilingual', case=False, na=False),
            df['result'].str.contains('Diploma awarded', case=False, na=False),
            df['result'].str.contains('Diploma not awarded', case=False, na=False)
        ],
        [1, 2, 0],
        default=np.nan
    )

    # Build data columns (duplicate values or use NaN where needed)
    df2 = pd.DataFrame({
        ('pt_ee_tok', 'PG'): np.nan,
        ('pt_ee_tok', 'FG'): df['pt_ee_tok'],
        #=====
        ('ee', 'sub')   : df['ee_sub'],
        ('ee', 'PG')    : np.nan,
        ('ee', 'FG')    : df['ee_fg'],
        ('tk', 'sub')   : df['tk_sub'],
        ('tk', 'PG')    : np.nan,
        ('tk', 'FG')    : df['tk_fg'],
        #=====
        ('pt_total', 'PG'): np.nan,
        ('pt_total', 'FG'): df['pt_total'],
        ('result', '1 = bilingual, 2 = pass, 0 = fail'): result_cal_value,            # fill with NaN to maintain structure
        ('result', 'FG'): df['result']
    }, index=df.index)
    
    # Assign MultiIndex columns
    df2.columns = multi_cols

    return df2
#==================================================================================================

def main():
    # Extract and save to CSV
    df = extract_results(PDF_PATH)
    # Format the dataframe
    df = reformat_results(df)
    # Format and save to CSV/Excel
    df_sub = reformat_results_sub(df)
    df_overall = reformat_results_overall(df)
    #==================================================================================================



    #==================================================================================================
    # Optionally clean column names
    #df_overall.columns = df_overall.columns.str.strip().str.lower()
    #df_sub.columns = df_sub.columns.str.strip().str.lower()

    # Determine common keys (example: candidate, name, birth_date)
    common_keys = ['session_number', 'personal_code', 'name', 'birth_date']

    # Merge on common keys (inner join to keep only matched, or outer to include all)
    merged_df = pd.merge(df_sub, df_overall, on=common_keys, how='outer')

    #print(merged_df)
    #sys.exit()
    # Export merged table
    df.to_csv('raw_' + CSV_PATH)
    merged_df.to_csv(CSV_PATH)
    merged_df.to_excel(XLSX_PATH)
    #==================================================================================================



    #df_2.to_excel('output_no_index.xlsx')
    #df_2.to_csv('output_no_index.csv')
    #df.to_csv(CSV_PATH, index=False)
    print(f"Extracted {len(df)} records and saved to {CSV_PATH}")
    print(f"Extracted {len(df)} records and saved to {XLSX_PATH}")


if __name__ == '__main__':
    main()
