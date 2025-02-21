import pandas as pd
import logging

logger = logging.getLogger(__name__)

class MedicationAnalyzer:
    """
    Analyzer for medication usage patterns.
    
    Analysis options:
    1. Show top 10 medications by usage - Get the most frequently prescribed medications
    """
    
    def __init__(self, data):
        self.data = data
        self.med_orders = self.data[self.data['품목'] == '03.투약료']
    
    @property
    def analysis_options(self):
        """Return available analysis options"""
        return {
            "title": "Show top 10 medications by usage",
            "description": "Get the most frequently prescribed medications",
            "method": lambda: self.get_top_medications(n=10)
        }
        
    def get_top_medications(self, n=10, by_month=False):
        """Get top n most prescribed medications"""
        logger.info(f"Getting top {n} medications...")
        
        if by_month:
            result = (self.med_orders.groupby(['month', '약품명칭'])['총사용량']
                   .sum()
                   .reset_index()
                   .sort_values(['month', '총사용량'], ascending=[True, False])
                   .groupby('month').head(n))
        else:
            result = (self.med_orders.groupby('약품명칭')['총사용량']
                    .sum()
                    .sort_values(ascending=False)
                    .head(n))
        
        logger.debug(f"Top medications results (first {min(n,2)} rows):")
        logger.debug("\n" + str(result.head(2)))
        
        return result 