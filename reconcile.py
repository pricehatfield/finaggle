def process_chase_data(df):
    """Process Chase data with detailed logging."""
    try:
        logger.info("Starting Chase data processing")
        logger.info(f"Input DataFrame shape: {df.shape}")
        logger.info(f"Input DataFrame columns: {df.columns.tolist()}")
        logger.info(f"Input DataFrame dtypes:\n{df.dtypes}")
        
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Log first few rows before processing
        logger.info("First few rows before processing:")
        logger.info(f"\n{df.head().to_string()}")
        
        # Ensure column names are strings and strip whitespace
        df.columns = df.columns.str.strip()
        
        # Map Chase columns to standard format
        column_mapping = {
            'Transaction Date': 'date',
            'Posting Date': 'date',
            'Description': 'description',
            'Amount': 'amount',
            'Type': 'type',
            'Category': 'category',
            'Memo': 'memo'
        }
        
        # Rename columns that exist in the mapping
        df = df.rename(columns={col: column_mapping[col] for col in df.columns if col in column_mapping})
        
        logger.info("After column mapping:")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"First few rows:\n{df.head().to_string()}")
        
        # Convert date column
        date_col = 'date'
        if date_col in df.columns:
            logger.info(f"Converting {date_col} column")
            logger.info(f"Date column values before conversion:\n{df[date_col].head()}")
            
            # Try to convert dates, handling multiple formats
            try:
                # First try MM/DD/YYYY format
                df[date_col] = pd.to_datetime(df[date_col], format='%m/%d/%Y', errors='coerce')
                # Then try YYYY-MM-DD format for any remaining NaT values
                mask = df[date_col].isna()
                if mask.any():
                    df.loc[mask, date_col] = pd.to_datetime(df.loc[mask, date_col], format='%Y-%m-%d', errors='coerce')
                
                logger.info(f"Date column values after conversion:\n{df[date_col].head()}")
                logger.info(f"Date column dtype: {df[date_col].dtype}")
            except Exception as e:
                logger.error(f"Error converting dates: {str(e)}")
                logger.error(f"Problematic date values:\n{df[date_col].head()}")
                raise
        
        # Convert amount column
        amount_col = 'amount'
        if amount_col in df.columns:
            logger.info(f"Converting {amount_col} column")
            logger.info(f"Amount column values before conversion:\n{df[amount_col].head()}")
            
            try:
                # Remove any currency symbols and convert to float
                df[amount_col] = df[amount_col].astype(str).str.replace('$', '').str.replace(',', '').astype(float)
                logger.info(f"Amount column values after conversion:\n{df[amount_col].head()}")
                logger.info(f"Amount column dtype: {df[amount_col].dtype}")
            except Exception as e:
                logger.error(f"Error converting amounts: {str(e)}")
                logger.error(f"Problematic amount values:\n{df[amount_col].head()}")
                raise
        
        # Ensure required columns exist
        required_columns = ['date', 'description', 'amount']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Select and order columns
        final_columns = ['date', 'description', 'amount', 'type', 'category', 'memo']
        df = df.reindex(columns=[col for col in final_columns if col in df.columns])
        
        logger.info("Final DataFrame structure:")
        logger.info(f"Shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"First few rows:\n{df.head().to_string()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in process_chase_data: {str(e)}")
        logger.error(f"DataFrame info:\n{df.info()}")
        logger.error(f"First few rows:\n{df.head().to_string()}")
        raise 