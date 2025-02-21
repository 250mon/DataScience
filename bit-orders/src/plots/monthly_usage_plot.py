import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import logging
from src.config import PLOT_SETTINGS

logger = logging.getLogger(__name__)

def plot_monthly_percentage(plot_data, col_name='유형'):
    """Plot monthly insurance type frequency trends"""
    logger.info("Generating monthly frequency trend plot...")
    
    # Set Korean font
    plt.rcParams['font.family'] = 'AppleGothic'
    plt.rcParams['axes.unicode_minus'] = False
    
    plt.figure(figsize=PLOT_SETTINGS['figure_size'])
    
    # Create color palette for different insurance types
    insurance_types = plot_data[col_name].unique()
    colors = sns.color_palette("husl", n_colors=len(insurance_types))
    
    # Create line plot for each insurance type
    for i, insurance in enumerate(insurance_types):
        subset = plot_data[plot_data[col_name] == insurance]
        sns.lineplot(data=subset, x='month', y='frequency', color=colors[i], 
                    label=str(insurance), linewidth=2)
    
    # Format x-axis ticks
    unique_months = sorted(plot_data['month'].unique())
    month_labels = [f"20{m[:2]}.{m[2:]}" for m in unique_months]
    plt.gca().set_xticks(range(len(unique_months)))
    plt.gca().set_xticklabels(month_labels, rotation=45)
    
    plt.title('Monthly Insurance Type Frequency Trends')
    plt.xlabel('Month')
    plt.ylabel('Frequency (%)')
    plt.legend(title='Insurance Type', bbox_to_anchor=(1.05, 1), 
              loc='upper left', fontsize=10, title_fontsize=11)
    
    plt.tight_layout()
    return plt

def plot_bar_monthly_counts(plot_data, col_name='유형'):
    """Plot monthly insurance type distribution"""
    logger.info("Generating monthly distribution plot...")
    
    # Set Korean font
    plt.rcParams['font.family'] = 'AppleGothic'
    plt.rcParams['axes.unicode_minus'] = False
    
    plt.figure(figsize=PLOT_SETTINGS['figure_size'])
    
    # Create color palette for different insurance types
    insurance_types = plot_data[col_name].unique()
    colors = sns.color_palette("husl", n_colors=len(insurance_types))
    
    # Create stacked bar plot
    pivot_data = plot_data.pivot(index='month', columns=col_name, values='count')
    pivot_data = pivot_data.fillna(0)
    
    # Sort months chronologically
    pivot_data.index = pd.to_numeric(pivot_data.index)
    pivot_data = pivot_data.sort_index()
    
    # Create stacked bar plot
    ax = pivot_data.plot(kind='bar', stacked=True, 
                        color=colors, figsize=PLOT_SETTINGS['figure_size'])
    
    # Format x-axis ticks
    month_labels = [f"20{str(m)[:2]}.{str(m)[2:]}" for m in pivot_data.index]
    plt.gca().set_xticklabels(month_labels, rotation=45)
    
    # Add value labels on the bars
    for c in ax.containers:
        ax.bar_label(c, fmt='%.0f', label_type='center')
    
    plt.title('Monthly Insurance Type Distribution')
    plt.xlabel('Month')
    plt.ylabel('Number of Cases')
    plt.legend(title='Insurance Type', bbox_to_anchor=(1.05, 1), 
              loc='upper left', fontsize=10, title_fontsize=11)
    
    plt.grid(True, linestyle='--', alpha=0.7, axis='y')
    plt.tight_layout()
    return plt 

def plot_line_monthly_counts_with_total(plot_data, col_name='약품명칭'):
    """Plot monthly order counts for specific items"""
    return plot_line_monthly_counts(plot_data, col_name, calc_total=True, total_only=False)

def plot_line_monthly_total_counts(plot_data, col_name='약품명칭'):
    """Plot monthly order counts for specific items"""
    return plot_line_monthly_counts(plot_data, col_name, calc_total=True, total_only=True)

def plot_line_monthly_counts(plot_data, col_name=None, calc_total=False, total_only=False):
    """Plot monthly order counts for specific items"""
    logger.info("Generating monthly order counts plot...")
    
    if col_name is None:
        logger.warning("No column name provided, skipping plot generation.")
        return None

    # Set Korean font
    plt.rcParams['font.family'] = 'AppleGothic'
    plt.rcParams['axes.unicode_minus'] = False
    
    plt.figure(figsize=PLOT_SETTINGS['figure_size'])
    
    # Create pivot table
    pivot_data = plot_data.pivot(index='month', columns=col_name, values='count')
    pivot_data = pivot_data.fillna(0)
    
    if calc_total and pivot_data.shape[1] > 1:
        # Add total sum column
        pivot_data['Total'] = pivot_data.sum(axis=1)
        if total_only:
            pivot_data = pivot_data[['Total']]
    
    # Create color palette for different items
    colors = sns.color_palette("husl", n_colors=len(pivot_data.columns))
    
    # Create line plot
    ax = pivot_data.plot(kind='line', color=colors, marker='o')
    
    # Format x-axis ticks
    month_labels = [f"20{m[:2]}.{m[2:]}" for m in pivot_data.index]
    plt.gca().set_xticks(range(len(month_labels)))
    plt.gca().set_xticklabels(month_labels, rotation=45)
    
    # Add value labels on the lines
    for line in ax.lines:
        y_data = line.get_ydata()
        x_data = line.get_xdata()
        for x, y in zip(x_data, y_data):
            if y > 0:  # Only show labels for non-zero values
                ax.annotate(f'{int(y)}', (x, y), textcoords="offset points",
                           xytext=(0,10), ha='center')
    
    plt.title('Monthly Order Counts')
    plt.xlabel('Month')
    plt.ylabel('Number of Orders')
    plt.legend(title='Order Item', bbox_to_anchor=(1.05, 1),
              loc='upper left', fontsize=10, title_fontsize=11)
    
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    return plt