from pathlib import Path
import logging

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Database settings
DB_NAME = "medical_orders.db"
DB_PATH = DATA_DIR / DB_NAME
DATA_SOURCE_DIR = DATA_DIR / "orders24"

# Mapping columns
COLUMN_MAPPING = {
    '재료/행위': '재료_행위',
    '원내/원외': '원내_원외'
}

# Required columns for data processing
REQUIRED_COLUMNS_AND_TYPES = [
    ('수가코드', 'str'),
    ('약품명칭', 'str'),
    ('유형', 'str'),
    ('총투여량', 'float'),
    ('투약일수', 'int'),
    ('총사용량', 'float'),
    ('일평균', 'float'),
    ('품목', 'str'),
    ('재료_행위', 'str'),
    ('원내_원외', 'str')
]

# Plot settings
PLOT_SETTINGS = {
    'figure_size': (15, 8),
    'rotation': 45,
    'bbox_inches': 'tight',
    'dpi': 300
}

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s:%(filename)s:%(lineno)d: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'medical_analyzer.log',
            'mode': 'a',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': True
        },
        'matplotlib': {
            'level': 'WARNING',
        },
        'PIL': {
            'level': 'WARNING',
        }
    }
}

# Create necessary directories
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True) 