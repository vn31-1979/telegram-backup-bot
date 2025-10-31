#!/bin/bash
# Backup Bot Launcher Script

SCRIPT_DIR="/root/tg_uploader_bot_ver1"
VENV_PATH="$SCRIPT_DIR/telegram_uploader_venv"
LOG_FILE="/var/log/telegram_backup_launcher.log"

# Функция логирования
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> $LOG_FILE
}

# Проверка существования директории
if [ ! -d "$SCRIPT_DIR" ]; then
    log "ERROR: Script directory not found: $SCRIPT_DIR"
    exit 1
fi

# Проверка виртуального окружения
if [ ! -d "$VENV_PATH" ]; then
    log "ERROR: Virtual environment not found: $VENV_PATH"
    exit 1
fi

# Проверка основного скрипта
if [ ! -f "$SCRIPT_DIR/main.py" ]; then
    log "ERROR: main.py not found in $SCRIPT_DIR"
    exit 1
fi

cd $SCRIPT_DIR

log "Starting Telegram Backup Bot..."

# Запуск с таймаутом (максимум 2 часа)
timeout 2h $VENV_PATH/bin/python3 $SCRIPT_DIR/main.py

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    log "Backup completed successfully"
elif [ $EXIT_CODE -eq 124 ]; then
    log "WARNING: Backup was terminated (timeout after 2 hours)"
else
    log "ERROR: Backup failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE
