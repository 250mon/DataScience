import sqlite3
import pandas as pd
from pathlib import Path
import logging.config
from src.config import (
    DB_PATH,
    REQUIRED_COLUMNS_AND_TYPES,
    COLUMN_MAPPING,
    LOGGING_CONFIG
)

"""
Database manager for medical order data.

This module handles:
    - Creating and managing SQLite database
    - Loading data from CSV/Excel files
    - Providing query interfaces for data analysis

Usage:
    1. Initialize:
        db_manager = MedicalDBManager('medical_orders.db')
    
    2. Load data:
        db_manager.load_files_to_db('data/orders24/')
    
    3. Query data:
        orders = db_manager.get_all_orders()
        monthly_orders = db_manager.get_orders_by_month('2309')
        insurance_dist = db_manager.get_insurance_distribution()
"""

# Initialize logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

class MedicalDBManager:
    def __init__(self, db_path=DB_PATH):
        """Initialize database manager"""
        self.db_path = db_path
        self._create_tables()
    
    def _create_tables(self):
        """Create necessary database tables if they don't exist"""
        with sqlite3.connect(self.db_path) as conn:
            # Create SQL column definitions from REQUIRED_COLUMNS_AND_TYPES
            columns = [
                f"{col} {self._get_sql_type(dtype)}"
                for col, dtype in REQUIRED_COLUMNS_AND_TYPES
            ]
            columns.extend([
                "id INTEGER PRIMARY KEY AUTOINCREMENT",
                "source_file TEXT",
                "month TEXT"
            ])
            
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS orders (
                    {', '.join(columns)}
                )
            """)
            conn.commit()
    
    def _get_sql_type(self, dtype):
        """Convert Python type to SQLite type"""
        type_mapping = {
            'int': 'INTEGER',
            'float': 'REAL',
            'str': 'TEXT'
        }
        return type_mapping.get(dtype, 'TEXT')
    
    def load_files_to_db(self, directory_path):
        """Load all CSV and Excel files from directory into database"""
        files = []
        for ext in ['*.csv', '*.xlsx']:
            files.extend(Path(directory_path).glob(ext))
        
        total_records = 0
        
        with sqlite3.connect(self.db_path) as conn:
            # Clear existing data
            conn.execute("DELETE FROM orders")
            
            for file_path in files:
                try:
                    # Try different encodings for CSV files
                    if file_path.suffix.lower() == '.csv':
                        try:
                            df = pd.read_csv(file_path, delimiter='\t', encoding='utf-16')
                        except:
                            try:
                                df = pd.read_csv(file_path, delimiter='\t', encoding='euc-kr')
                            except:
                                logger.error(f"Failed to read {file_path} with multiple encodings")
                                continue
                    else:  # Excel files
                        df = pd.read_excel(file_path)
                    
                    # Rename columns according to mapping
                    df = df.rename(columns=COLUMN_MAPPING)
                    
                    # Process each column according to its required type
                    for col, dtype in REQUIRED_COLUMNS_AND_TYPES:
                        if col not in df.columns:
                            df[col] = None
                        
                        if dtype in ['int', 'float']:
                            # Remove commas from numeric strings
                            if df[col].dtype == 'object':
                                df[col] = df[col].str.replace(',', '')
                            
                            # Convert to appropriate numeric type
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            
                            if dtype == 'int':
                                df[col] = df[col].fillna(0).astype(int)
                            else:  # float
                                df[col] = df[col].fillna(0.0)
                        else:
                            # trim whitespace from string columns
                            df[col] = df[col].str.strip()
                    
                    # Extract only the required columns
                    df = df[[col for col, _ in REQUIRED_COLUMNS_AND_TYPES]]
                    
                    # Add file info
                    df['source_file'] = file_path.name
                    df['month'] = file_path.stem
                    
                    # Insert data into SQLite
                    df.to_sql('orders', conn, if_exists='append', index=False)
                    
                    records = len(df)
                    total_records += records
                    logger.info(f"Loaded {records} records from {file_path.name}")
                    logger.debug("Sample data types:")
                    logger.debug(df.dtypes)
                    logger.debug("First 2 rows:")
                    logger.debug(df.head(2))
                    
                except Exception as e:
                    logger.error(f"Error loading {file_path}: {str(e)}")
                    raise  # Re-raise the exception for debugging
        
        logger.info(f"Total records loaded: {total_records}")
        return total_records
    
    def get_all_orders(self):
        """Retrieve all orders as a pandas DataFrame"""
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query("SELECT * FROM orders", conn)
    
    def get_orders_by_month(self, month):
        """Retrieve orders for a specific month"""
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(
                "SELECT * FROM orders WHERE month = ?",
                conn,
                params=(month,)
            )
    
    def get_insurance_distribution(self):
        """Get distribution of orders by insurance type"""
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(
                """
                SELECT month, 유형, COUNT(*) as count
                FROM orders
                GROUP BY month, 유형
                ORDER BY month, count DESC
                """,
                conn
            ) 