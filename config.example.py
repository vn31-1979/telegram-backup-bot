#!/usr/bin/env python3
"""
Configuration settings for Telegram Backup Bot
"""
import os
import logging
from datetime import datetime

# =============================================================================
# TELEGRAM CONFIGURATION
# =============================================================================

# Telegram API credentials
API_ID = 0000000
API_HASH = '00000000'
PHONE_NUMBER = '+7000000000'
TARGET_CHAT = -00000000
ERROR_CHAT = -00000  # Группа для уведомлений об ошибках

# =============================================================================
# FILE PROCESSING CONFIGURATION
# =============================================================================

# File extensions to search for
FILE_EXTENSIONS = ['*.7z', '*.7z.*', '*.zip', '*.tar.gz', '*.tar', '*.sql', '*.dump']

# Maximum file size for Telegram (2GB)
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024

# Chunk size for file upload (in bytes)
CHUNK_SIZE = 1024 * 1024  # Увеличили до 1MB для лучшей производительности

# =============================================================================
# UPLOAD SETTINGS
# =============================================================================

# Pause between file sends (in seconds)
PAUSE_BETWEEN_FILES = 3
PAUSE_FOR_LARGE_FILES = 10
PAUSE_VERY_LARGE_FILES = 25

# Size thresholds (in MB)
LARGE_FILE_THRESHOLD = 100
VERY_LARGE_FILE_THRESHOLD = 1000

# Retry settings
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 30

# =============================================================================
# CLEANUP SETTINGS
# =============================================================================

# Delete files immediately after successful upload
DELETE_AFTER_UPLOAD = True  # Установите False если не хотите удалять файлы

# =============================================================================
# PATHS AND FILES
# =============================================================================

# Session file name
SESSION_FILE = 'telegram_uploader.session'

# Sources file
SOURCES_FILE = 'sources.txt'

# Log file
LOG_FILE = '/var/log/telegram_backup_bot.log'

# Upload history file
UPLOAD_HISTORY_FILE = 'upload_history.csv'

# =============================================================================
# PROGRESS REPORTING SETTINGS
# =============================================================================

# Progress logging intervals
PROGRESS_LOG_INTERVAL = 15  # seconds
PROGRESS_PERCENT_INTERVAL = 10  # percent

# Telegram message frequency
TELEGRAM_PROGRESS_INTERVAL = 10  # files
TELEGRAM_LARGE_FILE_THRESHOLD = 500  # MB

# =============================================================================
# TELEthon OPTIMIZATION SETTINGS
# =============================================================================

# Telethon connection settings
CONNECTION_RETRIES = 5
TIMEOUT = 60
REQUEST_RETRIES = 5

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def format_size(size_bytes: float) -> str:
    """Форматирует размер в байтах в человеко-читаемый вид"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    if i == 0:  # Байты
        return f"{int(size_bytes)} B"
    elif i == 1:  # Килобайты
        return f"{size_bytes:.1f} KB"
    elif i == 2:  # Мегабайты  
        return f"{size_bytes:.1f} MB"
    else:  # Гигабайты и выше
        return f"{size_bytes:.2f} {size_names[i]}"

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

def setup_logging():
    """Configure logging with file and console handlers"""
    # Create log directory if it doesn't exist
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger('BackupBot')
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Suppress telethon internal logs
    telethon_logger = logging.getLogger('telethon')
    telethon_logger.setLevel(logging.WARNING)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler (append mode)
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8', mode='a')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Also configure root logger
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler]
    )
    
    return logger

# =============================================================================
# UPLOAD HISTORY FILE TEMPLATE
# =============================================================================

UPLOAD_HISTORY_HEADER = "filename,source_path,upload_date,upload_success,file_size_mb,telegram_message_id,deleted_after_upload\n"

# =============================================================================
# SOURCES FILE TEMPLATE
# =============================================================================

SOURCES_FILE_TEMPLATE = """# Network sources for backup files
# Format: path|username|password|mount_point
# - path: Windows network path (\\\\server\\share) or local path
# - username: Windows username (optional)
# - password: Windows password (optional) 
# - mount_point: Local mount point for network shares (optional)

# Examples:
# Windows network share with credentials
\\\\172.16.5.127\\_Backup_8TB|backup_user|password123|/mnt/backup_server

# Windows network share without credentials (uses guest/anonymous)
\\\\server2\\backup_folder|||/mnt/server2_backup

# Local Linux path (username/password ignored)
/backup/local_files|||

# Mounted network drive
/mnt/nas/backups|||

# Commented example (will be ignored)
# \\\\old_server\\backup|user|pass|/mnt/old_backup
"""
