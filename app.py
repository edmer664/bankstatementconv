"""
This file should parse all pdf files in the ./pdfs directory (bank statements)  and extract the following information: Date, Details, Credits, Debits, Balance.
the extracted information should be stored in a CSV file named '{original_filename}.csv' in the ./csv directory.
"""

import os
import camelot
import ghostscript
import pandas as pd
import glob
from pathlib import Path

def parse_bank_statements():
    """Parse all PDF files in the ./pdfs directory and extract bank statement data."""
    
    # Get all PDF files in the pdfs directory
    pdf_directory = "./pdfs"
    pdf_files = glob.glob(os.path.join(pdf_directory, "*.pdf"))
    
    if not pdf_files:
        print("No PDF files found in the ./pdfs directory")
        print("Please place your bank statement PDF files in the ./pdfs directory")
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
                
                # Try to identify and clean the table structure
                processed_df = process_table(df, i+1)
                
                if processed_df is not None and not processed_df.empty:
                    all_data.append(processed_df)
            
            # Combine all tables data
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Final cleaning
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
                print(f"✗ No suitable table data found in {pdf_file}")
                
        except Exception as e:
            print(f"✗ Error processing {pdf_file}: {str(e)}")

def process_table(df, table_num):
    """Process and clean individual table data."""
    try:
        # Create a copy to avoid modifying original
        df = df.copy()
        
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        if df.empty:
            return None
        
        # Reset index
        df = df.reset_index(drop=True)
        
        # Try to detect headers
        header_detected = False
        header_row = 0
        
        # Look for common banking terms in first few rows
        banking_keywords = ['date', 'detail', 'description', 'credit', 'debit', 'balance', 'amount', 'transaction']
        
        for row_idx in range(min(3, len(df))):
            row_text = ' '.join(str(df.iloc[row_idx]).lower())
            if sum(keyword in row_text for keyword in banking_keywords) >= 2:
                header_row = row_idx
                header_detected = True
                break
        
        # Set appropriate column names
        if header_detected:
            # Use detected headers
            new_columns = []
            for col in df.iloc[header_row]:
                col_str = str(col).strip().lower()
                if 'date' in col_str:
                    new_columns.append('Date')
                elif any(term in col_str for term in ['detail', 'description', 'particular']):
                    new_columns.append('Details')
                elif 'credit' in col_str:
                    new_columns.append('Credits')
                elif 'debit' in col_str:
                    new_columns.append('Debits')
                elif 'balance' in col_str:
                    new_columns.append('Balance')
                else:
                    new_columns.append(f'Column_{len(new_columns)+1}')
            
            df.columns = new_columns
            df = df.drop(df.index[:header_row+1])
        else:
            # Assign standard column names based on position
            num_cols = df.shape[1]
            if num_cols >= 5:
                df.columns = ['Date', 'Details', 'Credits', 'Debits', 'Balance'] + [f'Extra_{j}' for j in range(5, num_cols)]
            elif num_cols == 4:
                df.columns = ['Date', 'Details', 'Amount', 'Balance']
                # Split Amount into Credits/Debits based on sign or format
                df['Credits'] = ''
                df['Debits'] = ''
                # You might need to customize this logic based on your bank's format
            else:
                # Generic naming for unusual formats
                df.columns = [f'Column_{j+1}' for j in range(num_cols)]
        
        # Ensure we have the required columns
        required_columns = ['Date', 'Details', 'Credits', 'Debits', 'Balance']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
        
        # Select only required columns
        df = df[required_columns]
        
        # Clean data
        df = df.dropna(how='all')
        df = df[df['Date'].astype(str).str.strip() != '']
        
        # Basic data cleaning
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', '')
        
        return df if not df.empty else None
        
    except Exception as e:
        print(f"Error processing table {table_num}: {str(e)}")
        return None

if __name__ == "__main__":
    parse_bank_statements()
