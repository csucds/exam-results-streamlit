import os
import sys
import io

import numpy as np
import pandas as pd

import streamlit as st
import pdfplumber
from pathlib import Path
import tempfile

import ib_result_handler_summary as data_handler_sum
import ib_result_handler_subject as data_handler_sub
#import ib_result_consolidator as consolidator

class IBResultProcessor:
    def __init__(self):
        self.file_types = {
            'Results summary'       : 'IB Result (Student Level)',
            'Subject Results'       : 'IB Result (Subject Level)',
            '(EXTENDED ESSAY)'      : 'IB Result (Subject Level) - Extended Essay',
            '(THEORY OF KNOWLEDGE)' : 'IB Result (Subject Level) - Theory of Knowledge'
        }

        self.file_type_header_map = {
            'Results summary'       :   '',
            '(EXTENDED ESSAY)'      :   '_sub_ee',
            '(THEORY OF KNOWLEDGE)' :   '_sub_tk',
            'Subject Results'       :   '_sub'
        }

        self.merge_steps = {
            "Subject Results": {
                'merge_file_substring'  : '',
                'index_cols'            : ['session_number', 'personal_code', 'subject'],
                'merge_cols'            : ['pg', 'scaled_total'],
                'rename_map'            : {'pg': 'PG'}
            },
            "(EXTENDED ESSAY)": {
                'merge_file_substring'  : '(EXTENDED ESSAY)',
                'index_cols'            : ['session_number', 'personal_code', 'subject'],
                'merge_cols'            : ['pg'],
                'rename_map'            : {'pg': 'ee_pg'}
            },
            "(THEORY OF KNOWLEDGE)": {
                'merge_file_substring'  : '(THEORY OF KNOWLEDGE)',
                'index_cols'            : ['session_number', 'personal_code', 'subject'],
                'merge_cols'            : ['pg'],
                'rename_map'            : {'pg': 'tk_pg'}
            }
        }

        # Initialize session state
        if 'processed_files' not in st.session_state:
            st.session_state.processed_files = {key: None for key in self.file_types}
        
        if 'consolidated_df' not in st.session_state:
            st.session_state.consolidated_df = None
            
        if 'formatted_df' not in st.session_state:
            st.session_state.formatted_df = None

    # =============================================================================================


    # =============================================================================================
    # get header of the file
    def detect_file_type(self, pdf_path):
        """Detect the type of IB result file based on its first page content."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if not pdf.pages:
                    st.warning(f"Warning: Could not read pages from a PDF.")
                    return None
                first_page_text = pdf.pages[0].extract_text()
                for header in self.file_type_header_map:
                    if header in first_page_text:
                        return header
                print(first_page_text)
            return None
        except Exception as e:
            st.error(f"Error reading PDF: {str(e)}")
            return None

    def process_uploaded_file(self, uploaded_file):
        """Process the uploaded file and return its type"""
        if uploaded_file is not None:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                file_type = self.detect_file_type(tmp_file.name)
                
                print("======")
                print(file_type)
                print("======")
                #sys.exit()

                if file_type:
                    # Store the temp file path in session state
                    st.session_state.processed_files[file_type] = tmp_file.name
                    return file_type
                else:
                    st.error("Could not determine file type")
                    return None

    def consolidate_data(self):
        """Consolidates data using the file paths stored in session_state."""
        # You would adapt your existing consolidation logic to read from the paths
        # in st.session_state.processed_files.
        # This is a placeholder for your detailed logic.
        try:

            #======================================================================================
            #print("========= Process Summary File ==========")

            student_file = st.session_state.processed_files['Results summary']
            if not student_file:
                st.error("Student Level Result file is required for processing!")
                return None, None

            # --- This part should be adapted from your `ib_result_consolidator.py` ---
            # For demonstration, we'll create dummy dataframes.
            # In your real app, you would call your extraction and merging functions here.
            


            #======================================================================================
            # 1.0 Extract from student file
            df_main = data_handler_sum.extract_results(student_file)
            df_main = data_handler_sum.reformat_results(df_main)
            
            #======================================================================================
            # 1.2 Extract from Subject file
            df_sub = pd.DataFrame()
            for key in self.file_types:
                if key != next(iter(self.file_types)):
                    print(key)
                    subject_file = st.session_state.processed_files[key]
                    if subject_file:
                        print(key)
                        tmp_df_sub, str_subject_type = data_handler_sub.extract_results(subject_file)
                        #df_sub = pd.concat([df_sub, tmp_df_sub], ignore_index=True)
                        tmp_df_sub = data_handler_sub.reformat_results(tmp_df_sub)

                        #==========================================================================
                        # 1.2.1 Format from Subject file
                        step = self.merge_steps[key]
                        cols_to_use = step['index_cols'] + step['merge_cols']
                        #df_merge = df_sub[cols_to_use]
                        df_merge = tmp_df_sub[cols_to_use]

                        if step['rename_map']:
                            df_merge = df_merge.rename(columns=step['rename_map'])
                        df_merged = pd.merge(df_main, df_merge, on=step['index_cols'], how='left', suffixes=('', '_new'))
                        # If any columns are meant to update existing ones, do so
                        for col in step['merge_cols']:
                            target_col = step['rename_map'][col] if step['rename_map'] and col in step['rename_map'] else col
                            if target_col in df_main.columns and f"{target_col}_new" in df_merged.columns:
                                df_merged[target_col] = df_merged[f"{target_col}_new"].combine_first(df_merged[target_col])
                                df_merged.drop(columns=[f"{target_col}_new"], inplace=True)
                        #==========================================================================
                        df_main = df_merged.copy()
                        #df_sub = pd.concat([df_sub, tmp_df_sub], ignore_index=True)
                        #==========================================================================
            #df_sub = data_handler_sub.reformat_results(df_sub)
            #======================================================================================

            # Format and save to CSV/Excel
            #df_sub         = data_handler_sum.reformat_results_sub(df_main)
            #df_overall     = data_handler_sum.reformat_results_overall(df_main)
            df_sub          = data_handler_sum.reformat_results_sub(df_merged)
            df_overall      = data_handler_sum.reformat_results_overall(df_merged)
            df_merged_final = pd.merge(df_sub, df_overall, on=data_handler_sum.common_keys, how='outer')
            #======================================================================================
            return df_merged, df_merged_final

            # df_merged - Raw
            # df_merged_final - formatted
            #df_main = df_merged.copy()
            #formatted_df = df_merged_final.copy()
            #return df_main, formatted_df

        except Exception as e:
            st.error(f"Error during consolidation: {str(e)}")
            return None, None



    def create_streamlit_app(self):
        st.title("IB Result Processor")
        
        st.header("1. Upload Files")
        #st.sidebar.header("Upload Required Files")
        
        # --- CHANGE HERE: Allow multiple files upload ---
        uploaded_files = st.file_uploader(
            "Choose one or more PDF files",
            type="pdf",
            accept_multiple_files=True,
            key="file_uploader"
        )

        # --- CHANGE HERE: Process uploaded files in a loop ---
        if uploaded_files:
            # Use a set to avoid reprocessing the same file if the user interacts with the widget again
            if 'last_uploaded' not in st.session_state or set(f.name for f in uploaded_files) != st.session_state.last_uploaded:
                st.session_state.last_uploaded = set(f.name for f in uploaded_files)
                for uploaded_file in uploaded_files:
                    file_type = self.process_uploaded_file(uploaded_file)
                    if file_type:
                        st.success(f"Processed '{uploaded_file.name}' as: **{self.file_types[file_type]}**")
                    else:
                        st.warning(f"Could not determine file type for '{uploaded_file.name}'. The file will be ignored.")

        # Display the status of which file types have been successfully identified
        st.header("2. Check File Status")
        for file_type, display_name in self.file_types.items():
            status = "✅ Found" if st.session_state.processed_files[file_type] else "❌ Missing"
            st.write(f"{display_name}: {status}")
        
        # =============================================================================================
        # =============================================================================================
        # =============================================================================================

        st.header("3. Process and Display Results")
        if st.button("Consolidate Data"):
            #if st.session_state.processed_files['student_level']:
            if st.session_state.processed_files['Results summary']:
                with st.spinner("Processing... This may take a moment."):
                    consolidated_df, formatted_df = self.consolidate_data()
                    if consolidated_df is not None and formatted_df is not None:
                        st.session_state.consolidated_df = consolidated_df
                        st.session_state.formatted_df = formatted_df
                        st.success("Data processing complete!")
            else:
                st.error("Cannot process without the 'IB Result (Student Level)' file.")

        # =============================================================================================
        # =============================================================================================
        # =============================================================================================
        # Display results if they exist in the session state



        # =============================================================================================
        if st.session_state.get('formatted_df') is not None:
            st.subheader("Formatted Data")
            st.dataframe(st.session_state.formatted_df)
            
            # Download buttons
            @st.cache_data
            def convert_df_to_excel(df):
                return df.to_excel(index=False).encode('utf-8')
            def convert_df_to_excel_bytes(df):
                buffer = io.BytesIO()
                # Use an Excel writer with the buffer as the target
                #df_reset = df.reset_index()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    #df_reset.to_excel(writer, index=False)
                    #df_reset.to_excel(writer)
                    df.to_excel(writer)
                # Get the Excel file bytes
                return buffer.getvalue()

            excel_bytes_result_formatted = convert_df_to_excel_bytes(st.session_state.formatted_df) 
            #excel_data = convert_df_to_excel(st.session_state.formatted_df)
            st.download_button(
                label="Download Formatted Data as Excel",
                data=excel_bytes_result_formatted,
                file_name="formatted_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        # =============================================================================================
        if st.session_state.get('consolidated_df') is not None:
            st.subheader("Raw Aggregated Data")
            st.dataframe(st.session_state.consolidated_df)
            # Download buttons
            @st.cache_data
            def convert_df_to_csv(df):
                return df.to_csv(index=False).encode('utf-8')
            
            csv_data_result_raw = convert_df_to_csv(st.session_state.consolidated_df)
            st.download_button(
                label="Download Raw Aggregated Data as csv",
                data=csv_data_result_raw,
                file_name="raw_results.csv",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

def main():
    processor = IBResultProcessor()
    processor.create_streamlit_app()

if __name__ == "__main__":
    main()