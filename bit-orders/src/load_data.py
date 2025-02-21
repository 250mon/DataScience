from src.db_manager import MedicalDBManager
import argparse
from pathlib import Path
from src.config import (
    DATA_SOURCE_DIR,
    DB_PATH,
    LOGGING_CONFIG
)
import logging.config

"""
This script loads medical order data from CSV/Excel files into a SQLite database.

Usage:
    1. From command line:
        python load_data.py --data-dir data/orders24/ --db-path medical_orders.db
    
    2. With default settings (will use data/orders24/ directory):
        python load_data.py

The script will:
    - Create a SQLite database (if it doesn't exist)
    - Load all CSV and Excel files from the specified directory
    - Print summary statistics about the loaded data

Arguments:
    --data-dir: Directory containing the CSV/Excel files (default: data/orders24/)
    --db-path: Path where the SQLite database will be created (default: medical_orders.db)
"""

# Initialize logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Load medical order data into SQLite database')
    parser.add_argument('--data-dir', type=str, default=str(DATA_SOURCE_DIR),
                       help='Directory containing CSV/Excel files')
    parser.add_argument('--db-path', type=str, default=str(DB_PATH),
                       help='Path to SQLite database file')
    
    args = parser.parse_args()
    
    # Ensure data directory exists
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Error: Directory {data_dir} does not exist")
        return
    
    # Initialize database manager and load data
    db_manager = MedicalDBManager(args.db_path)
    try:
        total_records = db_manager.load_files_to_db(data_dir)
        print(f"\nSuccessfully loaded {total_records} records into database")
        
        # Print some basic statistics
        df = db_manager.get_all_orders()
        print("\nDatabase Summary:")
        print(f"Total months: {df['month'].nunique()}")
        print(f"Total insurance types: {df['유형'].nunique()}")
        print(f"Total departments: {df['품목'].nunique()}")
            
    except Exception as e:
        print(f"Error loading data: {str(e)}")

if __name__ == "__main__":
    main() 