import matplotlib.pyplot as plt
import seaborn as sns
import logging
from src.config import PLOT_SETTINGS

logger = logging.getLogger(__name__)

def plot_daily_usage(plot_data):
    """Plot average daily usage by insurance type"""
    logger.info("Generating daily usage plot...")
    
    # Set Korean font
    plt.rcParams['font.family'] = 'AppleGothic'
    plt.rcParams['axes.unicode_minus'] = False
    
    plt.figure(figsize=PLOT_SETTINGS['figure_size'])
    
    # Create bar plot with hue parameter and no legend
    sns.barplot(data=plot_data, x='유형', y='frequency', hue='유형',
                palette="husl", legend=False)
    
    plt.title('Average Daily Usage by Insurance Type')
    plt.xlabel('Insurance Type')
    plt.ylabel('Average Daily Usage')
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    return plt 