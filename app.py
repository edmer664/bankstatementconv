"""
This file parses all PDF files in the ./pdfs directory and extracts all table data.
All extracted tables are saved as CSV files named '{original_filename}.csv' in the ./csv directory.
"""

import os
import camelot
import ghostscript
import pandas as pd
import glob
from pathlib import Path

def parse_pdfs_to_csv():
    """Parse all PDF files in the ./pdfs directory and extract all table data to CSV."""
    
    # Get all PDF files in the pdfs directory
    pdf_directory = "./pdfs"
    pdf_files = glob.glob(os.path.join(pdf_directory, "*.pdf"))
    
    if not pdf_files:
        print("No PDF files found in the ./pdfs directory")
        print("Please place your PDF files in the ./pdfs directory")
        return
    
    print(f"Found {len(pdf_files)} PDF file(s) to process")
    
    # Iterate over each PDF file
    for pdf_file in pdf_files:
        print(f"\nProcessing: {pdf_file}")
        
        try:
            # Try lattice method first (for tables with borders)
            tables = camelot.read_pdf(pdf_file, pages='all', flavor='lattice')
            
            # If no tables found with lattice, try stream method
            if not tables:
                print("No tables found with lattice method, trying stream method...")
                tables = camelot.read_pdf(pdf_file, pages='all', flavor='stream')
            
            if not tables:
                print(f"No tables found in {pdf_file} with either method")
                continue
            
            print(f"Found {len(tables)} table(s)")
            
            # Create a list to store all extracted data
            all_data = []
            
            # Process each table found in the PDF
            for i, table in enumerate(tables):
                df = table.df
                print(f"Table {i+1} shape: {df.shape}")
                
                if df.empty:
                    continue
                
                # Clean the table data
                cleaned_df = clean_table(df, i+1)
                
                if cleaned_df is not None and not cleaned_df.empty:
                    all_data.append(cleaned_df)
            
            # Combine all tables data
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Final cleaning - remove completely empty rows
                combined_df = combined_df.dropna(how='all')
                combined_df = combined_df.reset_index(drop=True)
                
                # Create output filename
                original_filename = Path(pdf_file).stem
                output_file = f"./csv/{original_filename}.csv"
                
                # Ensure csv directory exists
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                
                # Save to CSV
                combined_df.to_csv(output_file, index=False)
                print(f"✓ Successfully saved data to {output_file}")
                print(f"✓ Extracted {len(combined_df)} rows")
                
                # Display first few rows as preview
                if len(combined_df) > 0:
                    print("\nPreview of extracted data:")
                    print(combined_df.head().to_string(index=False))
                
            else:
                print(f"✗ No table data found in {pdf_file}")
                
        except Exception as e:
            print(f"✗ Error processing {pdf_file}: {str(e)}")

def clean_table(df, table_num):
    """Clean individual table data without filtering."""
    try:
        # Create a copy to avoid modifying original
        df = df.copy()
        
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        if df.empty:
            return None
        
        # Reset index
        df = df.reset_index(drop=True)
        
        # Generate column names based on number of columns
        num_cols = df.shape[1]
        df.columns = [f'Column_{j+1}' for j in range(num_cols)]
        
        # Basic data cleaning - convert to string and strip whitespace
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', '')
        
        return df if not df.empty else None
        
    except Exception as e:
        print(f"Error processing table {table_num}: {str(e)}")
        return None

if __name__ == "__main__":
    parse_pdfs_to_csv()
