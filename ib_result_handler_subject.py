import re
import sys

import numpy as np
import pandas as pd

import pdfplumber

# Path to the PDF file
PDF_PATH = 'display_report_.pdf'
#PDF_PATH = 'display_report_ee.pdf'
#PDF_PATH = 'display_report_tk.pdf'

# Output CSV path
CSV_PATH = 'exam_results_subject_.csv'
#============================================================================
lst_pdf_path = [
    'display_report_.pdf',  
    'display_report_ee.pdf',
    'display_report_tk.pdf'
]

str_keyword_subject_type = "Subject Results"

str_normal_subject_stype    = ''
str_ee_subject_stype        = '(EXTENDED ESSAY)'
str_tk_subject_stype        = '(THEORY OF KNOWLEDGE)'

dict_fields_to_extract = {
    str_normal_subject_stype : {
        "pg"                :   "Predicted",
        "fg"                :   "Grade",
        "scaled_total"      :   "Scaled total",
        #======================================
        "candidate"         :   ""
    },

    str_ee_subject_stype : {
        "pg"                :   "Predicted grade",
        "fg"                :   "Grade",
        #======================================
        "candidate"         :   ""
    },

    str_tk_subject_stype : {
        "pg"                :   "Predicted grade",
        "fg"                :   "Grade",
        #======================================
        "candidate"         :   ""
    }
}

dict_skip_line_to_data = {
    str_normal_subject_stype    : 2,
    str_ee_subject_stype        : 1,
    str_tk_subject_stype        : 1,
}
#============================================================================





def pasrse_field_single(str_search_keyword, text):
    pattern = rf"{re.escape(str_search_keyword)}\s*(.+)"
    match = re.search(pattern, text)

    m_res = (match.group(1).strip()) if match else next(iter(dict_fields_to_extract))
    return m_res

def parse_page(text):
    """
    Parse the text of one PDF page and extract student exam results.
    Returns a dict of fields.
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    str_subject_type = ""
    data = {}
    dict_record_lv_top = {}
    lst_record_lv_subject = []


    #print(len(dict_fields_to_extract.get("normal", {})))
    # find its line index
    for i, l in enumerate(lines):

        if l.strip().startswith(str_keyword_subject_type):
            str_subject_type = pasrse_field_single(str_keyword_subject_type, l)
            #print('subject: ' + str_subject_type)
            #print(len(dict_fields_to_extract.get( (str_subject_type) , {})))
            continue

        if l.startswith('Predicted'):
            # Step 1: Get the sub-dictionary with key ''
            default_fields = dict_fields_to_extract.get(str_subject_type, {})
            # Step 2: Get values except the last one
            values_except_last = list(default_fields.values())[:-1]
            # Step 3: Join them into a string
            result_string = ' '.join(values_except_last)
            int_subject_pos = len(result_string.split())
            str_subject_name = " ".join(l.split(' ')[int_subject_pos:])

            #skip line(s) for non EE and TK subject
            raw_lines = lines[i + dict_skip_line_to_data.get(str_subject_type):]
            for line in raw_lines:
                if line.startswith("Page") or not line.strip():
                    break

                parts = line.split(' ', int_subject_pos-1)
                '''
                predicted_grade = parts[0]
                grade = parts[1]
                scaled_total = parts[2]
                candidate = parts[3]
                print("Predicted Grade:", predicted_grade)
                print("Grade:", grade)
                print("Scaled Total:", scaled_total)
                print("Candidate:", candidate)
                '''

                # Create new dict by zipping field keys to parts
                dict_tmp_reocrd             = {key: parts[i] for i, key in enumerate(dict_fields_to_extract.get(str_subject_type, {}))}
                dict_tmp_reocrd['subject']  = str_subject_name
                lst_record_lv_subject       = lst_record_lv_subject + [dict_tmp_reocrd]
                
    return lst_record_lv_subject, str_subject_type

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
            rec, str_subject_type = parse_page(text)
            #records.append(rec)
            records = records + rec

    # Normalize into DataFrame
    df = pd.json_normalize(records)
    return df, str_subject_type

def reformat_results(df):
    df[['session', 'candidate']]            = df['candidate'].str.split(' - ', n=1, expand=True)
    df[['subject_', 'sub']]                 = df['subject'].str.extract(r'^(.*?)(\b(?:SL|HL|EE|TK)\b.*)$')
    df[['session_number', 'personal_code']] = df['session'].str.extract(r'^(.+?)\s*(\([^)]+\))$')
    return df

def main():
    #df = extract_results(PDF_PATH)
    for str_path in lst_pdf_path:
        # Extract and save to CSV
        df, str_subject_type = extract_results(str_path)
        df = reformat_results(df)

        new_CSV_PATH = CSV_PATH.replace("subject_.csv", f"subject_{str_subject_type}.csv")
        #print(new_CSV_PATH)

        df.to_csv(new_CSV_PATH, index=False)
        print(f"Extracted {len(df)} records and saved to {CSV_PATH}")

if __name__ == '__main__':
    main()
