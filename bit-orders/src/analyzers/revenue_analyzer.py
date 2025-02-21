import pandas as pd
import logging

logger = logging.getLogger(__name__)

class RevenueAnalyzer:
    """
    Analyzer for revenue patterns by department.
    
    Analysis options:
    1. Show top 10 departments by revenue - Get the departments with highest total usage
    """
    
    def __init__(self, data):
        self.data = data
    
    @property
    def analysis_options(self):
        """Return available analysis options"""
        return {
            "title": "Show top 10 departments by revenue",
            "description": "Get the departments with highest total usage",
            "method": lambda: self.analyze_department_revenue().head(10)
        }
        
    def analyze_department_revenue(self, by_month=False):
        """Analyze revenue by department/category"""
        logger.info("Analyzing department revenue...")
        
        if by_month:
            result = (self.data.groupby(['month', '품목'])['총사용량']
                   .sum()
                   .unstack(fill_value=0))
        else:
            result = (self.data.groupby('품목')['총사용량']
                    .sum()
                    .sort_values(ascending=False))
        
        logger.debug("Department revenue results (first 2 rows):")
        logger.debug("\n" + str(result.head(2)))
        
        return result 