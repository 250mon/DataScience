import subprocess
import sys
from pathlib import Path
import logging.config
import traceback
from src.config import LOGGING_CONFIG, DATA_SOURCE_DIR, DB_PATH, OUTPUT_DIR, PLOT_SETTINGS
from src.order_analyzer import MedicalOrderAnalyzer
from src.analyzers.insurance_analyzer import InsuranceAnalyzer
from src.analyzers.medication_analyzer import MedicationAnalyzer
from src.analyzers.revenue_analyzer import RevenueAnalyzer
from src.analyzers.frequency_analyzer import FrequencyAnalyzer
from src.plots import daily_usage_plot, monthly_usage_plot

"""
Main interface script for the Medical Order Analysis System.

Usage:
    python run.py

This script provides a command-line interface to:
1. Load data into the database
2. Run analysis and generate reports
3. Exit the program

The script will guide you through the options and execute the selected functionality.
"""

# Initialize logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

def clear_screen():
    """Clear the console screen"""
    subprocess.run('cls' if sys.platform == 'win32' else 'clear', shell=True)

def print_header():
    """Print the program header"""
    print("\n" + "="*50)
    print("Medical Order Analysis System")
    print("="*50 + "\n")

def print_main_menu():
    """Print the main menu options"""
    print("\nPlease select an option:")
    print("1. Load data into database")
    print("2. Run analysis")
    print("3. Generate reports")
    print("0. Exit program")
    print("\nEnter your choice (0-4): ")

def print_report_menu():
    """Print the report generation sub-menu options"""
    print("\nReport Generation Options:")
    print("1. Generate monthly insurance type percentage trends")
    print("2. Generate average daily usage by insurance type")
    print("3. Generate monthly insurance type distribution")
    print("4. Return to main menu")
    print("0. Exit program")
    print("\nEnter your choice (0-4): ")

def load_data():
    """Execute the data loading process"""
    print("\nLoading data into database...")
    
    # Check if data directory exists
    if not DATA_SOURCE_DIR.exists():
        print(f"\nError: Data directory not found at {DATA_SOURCE_DIR}")
        print("Please ensure your data files are in the correct location.")
        return False
    
    try:
        # Use python -m to run the module
        subprocess.run([sys.executable, "-m", "src.load_data"], check=True)
        print("\nData loading completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nError during data loading: {str(e)}")
        return False

def run_analysis_menu(analyzer):
    """Handle analysis sub-menu"""
    # Initialize analyzers
    analyzers = {
        'medication': MedicationAnalyzer(analyzer.data),
        'insurance': InsuranceAnalyzer(analyzer.data),
        'revenue': RevenueAnalyzer(analyzer.data)
    }
    
    while True:
        clear_screen()
        print_header()
        
        # Collect all analysis options
        all_options = {}
        for choice_number, analyzer_obj in enumerate(analyzers.values()):
            all_options[str(choice_number + 1)] = analyzer_obj.analysis_options
        
        # Print menu
        print("\nAnalysis Options:")
        for choice_number, option in all_options.items():
            print(f"{choice_number}. {option['title']}")
        print("99. Return to main menu")
        print("0. Exit program")
        print("\nEnter your choice (0-99): ")
        
        choice = input().strip()
        
        if choice == '0':
            print("\nThank you for using Medical Order Analysis System!")
            sys.exit(0)
        elif choice == '99':
            break
        elif choice in all_options:
            option = all_options[choice]
            print(f"\n{option['title']}:")
            result = option['method']()
            print(result)
        else:
            print("\nInvalid choice. Please enter 0-4.")
        
        input("\nPress Enter to continue...")

def generate_reports_menu(analyzer):
    """Handle report generation sub-menu"""
    # Initialize frequency analyzer
    freq_analyzer = FrequencyAnalyzer(analyzer.data)
    
    while True:
        clear_screen()
        print_header()
        
        # Get all report options
        print("\nReport Generation Options:")
        print("1. Monthly order counts(Dosu)")
        print("2. Monthly order counts(PT)")
        print("99. Return to main menu")
        print("0. Exit program")
        print("\nEnter your choice (0-99): ")
        
        choice = input().strip()
        
        if choice == '0':
            print("\nThank you for using Medical Order Analysis System!")
            sys.exit(0)
        elif choice == '99':
            break
        elif choice in ['1', '2']:
            filename = None
            col_name = None
            
            if choice == '1':
                # Monthly order counts(Dosu)
                # Show submenu
                print("\nSelect filter:")
                print("1. 도수")
                print("2. 도수상담") 
                
                filter_choice = input("\nEnter choice (1-3): ").strip()
                
                if filter_choice == '1':
                    col_name = '약품명칭'
                    freq_analyzer.set_filter(col_name, ['도수치료9', '도수치료15'])
                    plot_func = monthly_usage_plot.plot_line_monthly_counts_with_total
                    filename = "monthly_order_counts_도수.png"
                elif filter_choice == '2':
                    col_name = '약품명칭'
                    freq_analyzer.set_filter(col_name, ['도수상담'])
                    plot_func = monthly_usage_plot.plot_line_monthly_counts
                    filename = "monthly_order_counts_도수상담.png"
                else:
                    print("Invalid choice, using no filters")
                    continue

            elif choice == '2':
                # Monthly order counts(PT)
                # Show submenu
                print("\nSelect filter:")
                print("1. 표층열")
                print("2. 기본물리치료") 
                print("3. 물리치료내원")
                
                filter_choice = input("\nEnter choice (1-3): ").strip()
                
                if filter_choice == '1':
                    col_name = '약품명칭'
                    freq_analyzer.set_filter(col_name, ['표층열치료'])
                    plot_func = monthly_usage_plot.plot_line_monthly_counts
                    filename = "monthly_order_counts_표층열.png"
                elif filter_choice == '2':
                    col_name = '약품명칭'
                    freq_analyzer.set_filter(col_name, ['표층열치료', '심층열치료'])
                    plot_func = monthly_usage_plot.plot_line_monthly_total_counts
                    filename = "monthly_order_counts_표층심층.png"
                elif filter_choice == '3':
                    col_name = '약품명칭'
                    freq_analyzer.set_filter(col_name, ['재진50%'])
                    plot_func = monthly_usage_plot.plot_line_monthly_counts
                    filename = "monthly_order_counts_물리치료내원.png"
                else:
                    print("Invalid choice, using no filters")
                    continue

            # Get plot data from analyzer
            plot_data = freq_analyzer.calculate_monthly_count(col_name)

            # Generate plot
            if col_name:
                plt = plot_func(plot_data, col_name)
            else:
                plt = plot_func(plot_data)
            plt.gcf().set_size_inches(20, 8)
            if filename:
                plt.savefig(
                    OUTPUT_DIR / filename,
                    bbox_inches=PLOT_SETTINGS['bbox_inches'],
                    dpi=PLOT_SETTINGS['dpi']
                )
            else:
                plt.show()
            
            plt.close()
            print(f"Plot saved as '{filename}'")
        else:
            print("\nInvalid choice. Please enter a valid option.")
        
        input("\nPress Enter to continue...")

def main():
    while True:
        clear_screen()
        print_header()
        
        # Check initial setup
        if not DATA_SOURCE_DIR.exists():
            print(f"Warning: Data directory not found at {DATA_SOURCE_DIR}")
        if not DB_PATH.exists():
            print("Warning: Database not found. Please load data first.")
        
        print_main_menu()
        
        try:
            choice = input().strip()
            
            if choice == '1':
                load_data()
            elif choice == '2':
                if not DB_PATH.exists():
                    print("\nError: Database not found! Please load data first.")
                else:
                    analyzer = MedicalOrderAnalyzer(DB_PATH)
                    run_analysis_menu(analyzer)
            elif choice == '3':
                if not DB_PATH.exists():
                    print("\nError: Database not found! Please load data first.")
                else:
                    analyzer = MedicalOrderAnalyzer(DB_PATH)
                    generate_reports_menu(analyzer)
            elif choice == '0':
                print("\nThank you for using Medical Order Analysis System!")
                sys.exit(0)
            else:
                print("\nInvalid choice. Please enter 0-4.")
            
            if choice != '2' and choice != '3':  # Don't show this for sub-menus
                input("\nPress Enter to continue...")
            
        except KeyboardInterrupt:
            print("\n\nProgram terminated by user.")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            input("\nPress Enter to continue...")

if __name__ == "__main__":
    main() 