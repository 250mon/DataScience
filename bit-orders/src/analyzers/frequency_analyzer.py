import pandas as pd
from typing import List
import logging

logger = logging.getLogger(__name__)

class FrequencyAnalyzer:
    """
    Analyzer for insurance frequency patterns.
    
    Report options:
    1. Monthly insurance type percentage trends - Shows how insurance type proportions change over time
    2. Average daily usage by insurance type - Shows average daily usage for each insurance type
    3. Monthly insurance type distribution - Shows absolute counts of insurance types by month
    """
    
    def __init__(self, data):
        self.data = data
    
    def set_filter(self, filter_col_name: str, filter_val_list: List[str]):
        """Set filter for the data"""
        self.filter_col_name = filter_col_name
        self.filter_val_list = filter_val_list
    
    def clear_filter(self):
        """Clear filter for the data"""
        self.filter_col_name = None
        self.filter_val_list = None
        
    @property
    def report_options(self):
        """Return available report generation options"""
        return {
            "monthly_order_counts": {
                "description": "Shows how many orders are made by month",
                "method": lambda: self.calculate_monthly_count('약품명칭'),
            },
            "monthly_frequency": {
                "description": "Shows the frequency by month",
                "method": lambda: self.calculate_monthly_frequency('유형'),
            },
            "average_daily_counts": {
                "description": "Shows average daily counts",
                "method": lambda: self.calculate_daily_averages('약품명칭'),
            }
        }

    def calculate_monthly_count(self, col_name):
        """Calculate monthly frequency for each insurance type"""
        logger.info("Calculating monthly insurance frequencies...")
        
        data = self.data
        if self.filter_col_name and self.filter_val_list:
            data = data[data[self.filter_col_name].apply(lambda x: any(val in str(x) for val in self.filter_val_list))]
            
        monthly_counts = (data.groupby(['month', col_name])['총사용량']
                         .sum()
                         .reset_index(name='count'))
    
        monthly_counts['month'] = monthly_counts['month'].astype(str).str.zfill(4)
        monthly_counts = monthly_counts.sort_values(['month', col_name])
        
        logger.debug("Monthly counts results (first 2 rows):")
        logger.debug("\n" + str(monthly_counts.head(2)))
        
        return monthly_counts   

    def calculate_monthly_frequency(self, col_name):
        """Calculate monthly frequency for each insurance type"""
        logger.info("Calculating monthly insurance frequencies...")
        
        monthly_counts = (self.data.groupby(['month', col_name])
                         .size()
                         .reset_index(name='count'))
    
        logger.info("Calculating monthly totals...")
        monthly_totals = (monthly_counts.groupby('month')['count']
                         .sum()
                         .reset_index(name='total'))
        
        monthly_freq = pd.merge(monthly_counts, monthly_totals, on='month')
        monthly_freq['frequency'] = (monthly_freq['count'] / monthly_freq['total']) * 100
        
        monthly_freq['month'] = monthly_freq['month'].astype(str).str.zfill(4)
        monthly_freq = monthly_freq.sort_values(['month', col_name])
        
        logger.debug("Monthly frequency calculation results (first 2 rows):")
        logger.debug("\n" + str(monthly_freq.head(2)))
        
        return monthly_freq
    
    def calculate_daily_averages(self, col_name):
        """Calculate daily average frequency for each insurance type"""
        logger.info("Calculating daily average frequencies...")
        
        daily_avg = (self.data.groupby(col_name)
                    .agg({'일평균': 'mean'})
                    .reset_index()
                    .rename(columns={'일평균': 'frequency'}))
        
        logger.debug("Daily average calculation results:")
        logger.debug("\n" + str(daily_avg))
        
        return daily_avg