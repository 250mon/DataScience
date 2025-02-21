import pandas as pd
from src.db_manager import MedicalDBManager
from src.config import DB_PATH, LOGGING_CONFIG
from src.plots.daily_usage_plot import plot_daily_usage
import logging.config

# Initialize logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

class MedicalOrderAnalyzer:
    def __init__(self, db_path=DB_PATH):
        """Initialize analyzer with database connection"""
        self.db_manager = MedicalDBManager(db_path)
        self.data = self.db_manager.get_all_orders()
        logger.info(f"Loaded {len(self.data)} records from database")
        logger.debug("First 2 rows of loaded data:")
        logger.debug("\n" + str(self.data.head(2)))
    
    def refresh_data(self):
        """Refresh data from database"""
        self.data = self.db_manager.get_all_orders()
        logger.info("Data refreshed from database")