#!/bin/bash
# Install dependencies for Telegram Backup Bot

echo "Installing system dependencies..."
sudo apt update
sudo apt install -y python3-pip cifs-utils

echo "Installing Python packages..."
pip3 install telethon==1.28.5

echo "Creating necessary directories..."
sudo mkdir -p /mnt/telegram_backup
sudo mkdir -p /var/log/

echo "Setting up log file..."
sudo touch /var/log/telegram_backup_bot.log
sudo chmod 644 /var/log/telegram_backup_bot.log

echo "Installation completed!"
echo "Please edit config.py with your Telegram credentials"
echo "and create sources.txt with your backup sources"
