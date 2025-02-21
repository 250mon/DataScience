import pandas as pd
import logging

logger = logging.getLogger(__name__)

class InsuranceAnalyzer:
    """
    Analyzer for insurance type patterns.
    
    Analysis options:
    1. Show top 10 insurance types by count - Get the most common insurance types
    """
    
    def __init__(self, data):
        self.data = data
    
    @property
    def analysis_options(self):
        """Return available analysis options"""
        return {
            "title": "Show top 10 insurance types by count",
            "description": "Get the most common insurance types",
            # "method": lambda: self.analyze_total_distribution()
            "method": lambda: self.analyze_monthly_distribution()
        }
    
    def analyze_monthly_distribution(self):
        """Analyze insurance distribution by month"""
        logger.info("Analyzing monthly insurance distribution...")
        
        distribution = self.data.groupby(['month', '유형']).size().unstack(fill_value=0)
        
        logger.debug("Monthly insurance distribution (first 2 rows):")
        logger.debug("\n" + str(distribution.head(2)))
        
        return distribution
    
    def analyze_total_distribution(self):
        """Analyze total insurance distribution"""
        logger.info("Analyzing total insurance distribution...")
        
        distribution = self.data['유형'].value_counts()
        
        logger.debug("Total insurance distribution:")
        logger.debug("\n" + str(distribution))
        
        return distribution 