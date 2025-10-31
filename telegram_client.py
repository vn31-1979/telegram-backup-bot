#!/usr/bin/env python3
"""
Telegram client for file uploading
"""
import os
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError

from config import (
    API_ID, API_HASH, PHONE_NUMBER, TARGET_CHAT, ERROR_CHAT,
    SESSION_FILE, MAX_RETRY_ATTEMPTS, RETRY_DELAY,
    CHUNK_SIZE, PAUSE_BETWEEN_FILES, PAUSE_FOR_LARGE_FILES,
    PAUSE_VERY_LARGE_FILES, LARGE_FILE_THRESHOLD, VERY_LARGE_FILE_THRESHOLD,
    PROGRESS_LOG_INTERVAL, PROGRESS_PERCENT_INTERVAL,
    TELEGRAM_PROGRESS_INTERVAL, TELEGRAM_LARGE_FILE_THRESHOLD,
    CONNECTION_RETRIES, TIMEOUT, REQUEST_RETRIES,
    UPLOAD_HISTORY_FILE, format_size
)

logger = logging.getLogger('BackupBot.telegram_client')

class TelegramUploader:
    """Handles Telegram file uploads with retry logic and progress tracking"""
    
    def __init__(self):
        self.client = None
        self.connected = False
        self.upload_history = []
    
    async def initialize(self) -> bool:
        """Initialize Telegram client"""
        try:
            logger.info("Initializing Telegram client...")
            
            self.client = TelegramClient(
                SESSION_FILE,
                API_ID,
                API_HASH,
                device_model="Backup Server",
                system_version="Debian Linux",
                app_version="2.0",
                connection_retries=CONNECTION_RETRIES,
                timeout=TIMEOUT,
                request_retries=REQUEST_RETRIES
            )
            
            await self.client.start(phone=PHONE_NUMBER)
            
            # Verify connection
            me = await self.client.get_me()
            logger.info(f"✅ Connected as: {me.first_name} (@{me.username})")
            
            self.connected = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Telegram client: {e}")
            await self.send_error_notification(f"Failed to initialize Telegram client: {str(e)}")
            return False
    
    async def send_message(self, text: str, chat_id: int = None):
        """Send text message to target chat"""
        if chat_id is None:
            chat_id = TARGET_CHAT
            
        try:
            if not self.connected:
                logger.error("Telegram client not connected")
                return False
            
            await self.client.send_message(chat_id, text)
            logger.debug(f"Message sent to {chat_id}: {text[:100]}...")
            return True
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    async def send_error_notification(self, error_message: str):
        """Send error notification to error chat"""
        try:
            message = f"🚨 BACKUP BOT ERROR\n\n{error_message}\n\n⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            await self.send_message(message, ERROR_CHAT)
            logger.info("Error notification sent")
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")
    
    def format_eta(self, seconds: float) -> str:
        """Format ETA in human readable format (e.g., '3m 25s')"""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
    
    def record_upload_history(self, file_info: Dict, success: bool, message_id: int = None):
        """Record upload attempt in history file"""
        logger.info(f"📝 Starting history recording for: {file_info['name']} (success: {success})")
        
        try:
            # Prepare data
            filename = file_info['name']
            source_path = file_info['path']
            upload_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            upload_success = "TRUE" if success else "FALSE"
            
            # Get file size - ensure we have the correct value
            file_size_bytes = file_info.get('size_bytes', 0)
            file_size_mb = file_info.get('size_mb', 0)
            
            # If size_mb is 0 but we have bytes, recalculate
            if file_size_mb == 0 and file_size_bytes > 0:
                file_size_mb = file_size_bytes / (1024 * 1024)
                logger.info(f"🔄 Recalculated size from bytes: {file_size_mb:.2f} MB")
            
            file_size_mb_str = f"{file_size_mb:.2f}"
            telegram_message_id = str(message_id) if message_id else ""
            
            logger.info(f"📊 File size for history: {file_size_mb_str} MB (from {file_size_bytes} bytes)")
            
            # Create row data
            row_data = [
                filename,
                source_path,
                upload_date,
                upload_success,
                file_size_mb_str,
                telegram_message_id
            ]
            
            # Manual CSV formatting to ensure no extra characters
            csv_line = ','.join(row_data) + '\n'
            
            # Check if file exists
            file_exists = os.path.exists(UPLOAD_HISTORY_FILE)
            logger.info(f"📁 History file exists: {file_exists}, path: {UPLOAD_HISTORY_FILE}")
            
            # Write to file
            with open(UPLOAD_HISTORY_FILE, 'a', encoding='utf-8') as f:
                if not file_exists:
                    logger.info("🆕 Creating new history file with header")
                    header = "filename,source_path,upload_date,upload_success,file_size_mb,telegram_message_id\n"
                    f.write(header)
                    logger.info(f"📋 Header written: {header.strip()}")
                
                f.write(csv_line)
                logger.info(f"✅ History recorded: {csv_line.strip()}")
            
            # Verify file was written
            if os.path.exists(UPLOAD_HISTORY_FILE):
                file_size = os.path.getsize(UPLOAD_HISTORY_FILE)
                logger.info(f"📊 History file size: {file_size} bytes")
                
                # Read last line to verify
                with open(UPLOAD_HISTORY_FILE, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    logger.info(f"📈 Total lines in history: {len(lines)}")
                    if lines:
                        logger.info(f"📄 Last line: {lines[-1].strip()}")
            
        except PermissionError as e:
            logger.error(f"❌ Permission denied writing history file: {e}")
        except Exception as e:
            logger.error(f"❌ Error recording upload history: {e}")
            logger.error(f"File info: {file_info}")
    
    async def send_file_with_progress(self, file_info: Dict, retry_count: int = 0) -> bool:
        """Send file to Telegram with progress tracking and retry logic"""
        try:
            file_path = file_info['path']
            
            if not self.connected:
                logger.error("Telegram client not connected")
                return False
            
            # Check if file is too large
            if file_info['is_too_large']:
                error_msg = (
                    f"File {file_info['name']} is too large "
                    f"({file_info['size_gb']:.2f}GB). Maximum: 2GB"
                )
                logger.warning(error_msg)
                await self.send_message(f"⚠️ {error_msg}")
                self.record_upload_history(file_info, False)
                return False
            
            # Upload start info
            retry_info = f" (retry {retry_count})" if retry_count > 0 else ""
            
            # Format size for logging
            size_display = format_size(file_info['size_bytes'])
            logger.info(
                f"Starting upload{retry_info}: {file_info['name']} "
                f"from {file_info['source']} "
                f"Size: {size_display}"
            )
            
            # Progress tracking
            upload_start_time = datetime.now()
            last_logged_time = upload_start_time
            last_logged_percent = -PROGRESS_PERCENT_INTERVAL
            
            def progress_callback(current: int, total: int):
                nonlocal last_logged_time, last_logged_percent
                if total > 0:
                    current_time = datetime.now()
                    elapsed = (current_time - upload_start_time).total_seconds()
                    percent = (current / total) * 100
                    
                    # Log based on time interval or percent interval
                    time_elapsed = (current_time - last_logged_time).total_seconds()
                    percent_elapsed = int(percent) - last_logged_percent
                    
                    if (time_elapsed >= PROGRESS_LOG_INTERVAL or 
                        percent_elapsed >= PROGRESS_PERCENT_INTERVAL or 
                        current == total):
                        
                        speed = current / elapsed / (1024*1024) if elapsed > 0 else 0
                        remaining_time = (total - current) / (current / elapsed) if current > 0 else 0
                        
                        current_mb = current/(1024*1024)
                        total_mb = total/(1024*1024)
                        
                        logger.info(
                            f"Upload progress: {current_mb:.1f}/"
                            f"{total_mb:.1f} MB ({percent:.1f}%) "
                            f"Speed: {speed:.1f} MB/s "
                            f"ETA: {self.format_eta(remaining_time)}"
                        )
                        
                        last_logged_time = current_time
                        last_logged_percent = int(percent)
            
            # Prepare caption with formatted size
            size_display = format_size(file_info['size_bytes'])
            caption = (
                f"📁 File: {file_info['name']}\n"
                f"💾 Size: {size_display}\n"
                f"📡 Source: {file_info['source']}\n"
                f"🔄 Modified: {file_info['modification_time']}\n"
                f"🖥️ Server: Backup Server"
            )
            
            # Send file
            logger.info(f"📤 Sending file to Telegram: {file_info['name']}")
            message = await self.client.send_file(
                TARGET_CHAT,
                file_path,
                caption=caption,
                progress_callback=progress_callback,
                part_size=CHUNK_SIZE,
                force_document=True
            )
            
            # Calculate and log upload statistics
            upload_time = (datetime.now() - upload_start_time).total_seconds()
            speed = file_info['size_mb'] / upload_time if upload_time > 0 else 0
            
            success_msg = (
                f"✅ File {file_info['name']} from {file_info['source']} "
                f"successfully sent ({size_display} "
                f"in {self.format_eta(upload_time)}, {speed:.1f} MB/s)"
            )
            logger.info(success_msg)
            
            # Record successful upload
            logger.info(f"💾 Recording successful upload in history")
            self.record_upload_history(file_info, True, message.id)
            return True
            
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"⏳ Flood wait required. Waiting {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            return await self.send_file_with_progress(file_info, retry_count)
            
        except RPCError as e:
            if retry_count < MAX_RETRY_ATTEMPTS:
                retry_delay = RETRY_DELAY * (retry_count + 1)
                logger.warning(
                    f"🔄 RPC Error: {e}, "
                    f"retry {retry_count + 1}/{MAX_RETRY_ATTEMPTS} "
                    f"in {retry_delay} seconds"
                )
                await asyncio.sleep(retry_delay)
                return await self.send_file_with_progress(file_info, retry_count + 1)
            else:
                error_msg = f"❌ RPC Error after {MAX_RETRY_ATTEMPTS} attempts: {e}"
                logger.error(error_msg)
                logger.info("📝 Recording failed upload in history")
                self.record_upload_history(file_info, False)
                await self.send_error_notification(f"Failed to upload {file_info['name']}: {e}")
                return False
                
        except Exception as e:
            if retry_count < MAX_RETRY_ATTEMPTS:
                retry_delay = RETRY_DELAY * (retry_count + 1)
                logger.warning(
                    f"🔄 Error: {e}, "
                    f"retry {retry_count + 1}/{MAX_RETRY_ATTEMPTS} "
                    f"in {retry_delay} seconds"
                )
                await asyncio.sleep(retry_delay)
                return await self.send_file_with_progress(file_info, retry_count + 1)
            else:
                error_msg = f"❌ Error after {MAX_RETRY_ATTEMPTS} attempts: {e}"
                logger.error(error_msg)
                logger.info("📝 Recording failed upload in history")
                self.record_upload_history(file_info, False)
                await self.send_error_notification(f"Failed to upload {file_info['name']}: {e}")
                return False
    
    def get_pause_time(self, file_size_mb: float) -> int:
        """Determine pause time based on file size"""
        if file_size_mb > VERY_LARGE_FILE_THRESHOLD:
            return PAUSE_VERY_LARGE_FILES
        elif file_size_mb > LARGE_FILE_THRESHOLD:
            return PAUSE_FOR_LARGE_FILES
        else:
            return PAUSE_BETWEEN_FILES
    
    async def send_files_batch(self, files: List[Dict]) -> Dict:
        """
        Send batch of files with progress reporting
        Returns statistics dictionary
        """
        if not files:
            logger.warning("No files to upload")
            return {'successful': 0, 'failed': 0, 'total': 0}
        
        logger.info(f"Starting upload of {len(files)} files")
        
        successful_uploads = 0
        failed_uploads = []
        total_uploaded_bytes = 0
        
        for i, file_info in enumerate(files, 1):
            try:
                # Progress reporting - less frequent
                should_report = (
                    i % TELEGRAM_PROGRESS_INTERVAL == 1 or 
                    file_info['size_mb'] > TELEGRAM_LARGE_FILE_THRESHOLD or 
                    i == len(files)
                )
                
                if should_report:
                    progress_msg = (
                        f"📊 Progress: {i}/{len(files)}\n"
                        f"📁 Current: {file_info['name']}\n"
                        f"📡 Source: {file_info['source']}\n"
                        f"💾 Size: {format_size(file_info['size_bytes'])}\n"
                        f"✅ Successful: {successful_uploads}\n"
                        f"❌ Failed: {len(failed_uploads)}"
                    )
                    await self.send_message(progress_msg)
                
                # Upload file
                logger.info(f"🔄 Processing file {i}/{len(files)}: {file_info['name']}")
                if await self.send_file_with_progress(file_info):
                    successful_uploads += 1
                    total_uploaded_bytes += file_info['size_bytes']
                    logger.info(f"✅ Uploaded {i}/{len(files)}: {file_info['name']}")
                else:
                    failed_info = {
                        'name': file_info['name'],
                        'source': file_info['source'],
                        'size_bytes': file_info['size_bytes'],
                        'error': 'Upload failed'
                    }
                    failed_uploads.append(failed_info)
                    logger.warning(f"❌ Failed {i}/{len(files)}: {file_info['name']}")
                
                # Pause between files
                pause_time = self.get_pause_time(file_info['size_mb'])
                if pause_time > 0:
                    logger.info(f"⏸️ Pausing for {pause_time} seconds")
                    await asyncio.sleep(pause_time)
                
            except Exception as e:
                error_msg = f"Unexpected error processing file {file_info['name']}: {e}"
                logger.error(error_msg)
                failed_info = {
                    'name': file_info['name'],
                    'source': file_info['source'],
                    'size_bytes': file_info['size_bytes'],
                    'error': str(e)
                }
                failed_uploads.append(failed_info)
                logger.info("📝 Recording failed upload in history (batch error)")
                self.record_upload_history(file_info, False)
                await self.send_error_notification(f"Unexpected error with {file_info['name']}: {e}")
        
        # Prepare results
        total_uploaded_gb = total_uploaded_bytes / (1024**3)
        results = {
            'successful': successful_uploads,
            'failed': len(failed_uploads),
            'total': len(files),
            'total_uploaded_bytes': total_uploaded_bytes,
            'total_uploaded_gb': total_uploaded_gb,
            'failed_uploads': failed_uploads
        }
        
        logger.info(f"Upload batch completed: {successful_uploads}/{len(files)} successful")
        logger.info(f"📊 Final results: {results}")
        return results
    
    async def disconnect(self):
        """Disconnect Telegram client"""
        if self.client and self.connected:
            try:
                await self.client.disconnect()
                self.connected = False
                logger.info("Telegram client disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Telegram client: {e}")
                await self.send_error_notification(f"Error disconnecting Telegram client: {e}")
