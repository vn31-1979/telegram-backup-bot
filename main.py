#!/usr/bin/env python3
"""
Main script for Telegram Backup Bot
"""
import os
import sys
import asyncio
import logging
from datetime import datetime

# Add current directory to path for module imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import setup_logging, SOURCES_FILE, SOURCES_FILE_TEMPLATE, UPLOAD_HISTORY_FILE, format_size, DELETE_AFTER_UPLOAD
from network_mount import NetworkMountManager
from file_processor import FileProcessor
from telegram_client import TelegramUploader
from cleanup_manager import CleanupManager

logger = logging.getLogger('BackupBot.main')

class BackupBot:
    """Main backup bot class coordinating all components"""
    
    def __init__(self):
        self.logger = logger
        self.mount_manager = NetworkMountManager()
        self.file_processor = FileProcessor()
        self.telegram_uploader = TelegramUploader()
        self.cleanup_manager = CleanupManager()
        self.sources = []
    
    def load_and_prepare_sources(self) -> bool:
        """Load sources from file and prepare network mounts"""
        try:
            # Check if sources file exists
            if not os.path.exists(SOURCES_FILE):
                self.logger.error(f"Sources file not found: {SOURCES_FILE}")
                self.create_sample_sources_file()
                return False
            
            # Read sources file
            with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            if not lines:
                self.logger.error(f"No valid sources found in {SOURCES_FILE}")
                return False
            
            self.logger.info(f"Loaded {len(lines)} sources from {SOURCES_FILE}")
            
            # Parse and prepare each source
            prepared_sources = []
            for line in lines:
                path, username, password, mount_point = self.mount_manager.parse_source_line(line)
                
                if not path:
                    self.logger.warning(f"Skipping invalid source line: {line}")
                    continue
                
                # Prepare source (mount if necessary)
                accessible_path = self.mount_manager.prepare_source(line)
                if accessible_path:
                    source_name = f"{path}→{os.path.basename(accessible_path)}" if path != accessible_path else path
                    prepared_sources.append((source_name, accessible_path))
                    self.logger.info(f"✅ Source prepared: {path} -> {accessible_path}")
                else:
                    self.logger.error(f"❌ Failed to prepare source: {path}")
            
            self.sources = prepared_sources
            self.logger.info(f"Successfully prepared {len(self.sources)} sources")
            return len(self.sources) > 0
            
        except Exception as e:
            self.logger.error(f"Error loading sources: {e}")
            return False
    
    async def handle_source_loading_error(self, error_message: str):
        """Handle source loading errors and send notifications"""
        self.logger.error(error_message)
        await self.telegram_uploader.send_error_notification(error_message)
    
    def create_sample_sources_file(self):
        """Create sample sources.txt file with template"""
        try:
            with open(SOURCES_FILE, 'w', encoding='utf-8') as f:
                f.write(SOURCES_FILE_TEMPLATE)
            self.logger.info(f"📝 Created sample {SOURCES_FILE}. Please edit it with your sources.")
        except Exception as e:
            self.logger.error(f"Error creating sample sources file: {e}")
    
    def check_read_only_filesystems(self, files: list) -> list:
        """Check which filesystems are read-only and return list of read-only directories"""
        read_only_dirs = set()
        
        for file_info in files:
            file_dir = os.path.dirname(file_info['path'])
            if file_dir not in read_only_dirs:
                try:
                    # Check if directory is writable
                    if not os.access(file_dir, os.W_OK):
                        read_only_dirs.add(file_dir)
                        self.logger.warning(f"🗑️ Read-only directory detected: {file_dir}")
                except Exception as e:
                    self.logger.warning(f"🗑️ Could not check permissions for {file_dir}: {e}")
        
        return list(read_only_dirs)
    
    async def send_startup_message(self, files_summary: dict, files: list):
        """Send startup message with file discovery summary and full file list"""
        # Format total size properly
        total_size_bytes = files_summary.get('total_size_bytes', 0)
        total_size_display = format_size(total_size_bytes)
        
        # Add info about auto-deletion
        deletion_info = "🗑️ AUTO-DELETE: ENABLED" if DELETE_AFTER_UPLOAD else "🗑️ AUTO-DELETE: DISABLED"
        
        # Check for read-only filesystems
        read_only_dirs = self.check_read_only_filesystems(files)
        read_only_warning = ""
        if read_only_dirs and DELETE_AFTER_UPLOAD:
            read_only_warning = "\n⚠️ WARNING: Some sources are read-only (files will not be deleted)"
        
        # Start message with basic summary
        message = (
            f"🚀 BACKUP BOT STARTED\n\n"
            f"📊 Discovery Summary:\n"
            f"📁 Total Files: {files_summary['total_files']}\n"
            f"💾 Total Size: {total_size_display}\n"
            f"📡 Sources: {files_summary['sources_count']}\n"
            f"📈 Large Files: {files_summary['large_files']}\n"
            f"⚠️ Too Large: {files_summary['too_large_files']}\n"
            f"{deletion_info}{read_only_warning}\n\n"
            f"🖥️ Server: {os.uname().nodename}\n"
            f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        await self.telegram_uploader.send_message(message)
        
        # Send sources overview
        if files_summary['sources_stats']:
            sources_msg = "📡 SOURCES OVERVIEW:\n\n"
            for source, stats in files_summary['sources_stats'].items():
                source_size_display = format_size(stats['total_size_bytes'])
                sources_msg += f"• {source}\n  📁 {stats['file_count']} files, {source_size_display}\n\n"
            
            await self.telegram_uploader.send_message(sources_msg)
        
        # Send detailed file list
        await self.send_detailed_file_list(files)
    
    async def send_detailed_file_list(self, files: list):
        """Send detailed list of all files to be uploaded"""
        if not files:
            await self.telegram_uploader.send_message("📋 No files found for upload")
            return
        
        self.logger.info(f"📤 Preparing to send detailed file list with {len(files)} files")
        
        # Sort files by size (smallest first)
        files_sorted = sorted(files, key=lambda x: x['size_bytes'])
        
        # Split files into chunks to avoid Telegram message length limits
        chunk_size = 20  # Reduced chunk size for better formatting
        file_chunks = [files_sorted[i:i + chunk_size] for i in range(0, len(files_sorted), chunk_size)]
        
        self.logger.info(f"📦 Split file list into {len(file_chunks)} chunks")
        
        for chunk_index, file_chunk in enumerate(file_chunks, 1):
            file_list_msg = f"📋 FILES FOR UPLOAD"
            if len(file_chunks) > 1:
                file_list_msg += f" (Part {chunk_index}/{len(file_chunks)})"
            file_list_msg += ":\n\n"
            
            for i, file_info in enumerate(file_chunk, 1):
                file_size = format_size(file_info['size_bytes'])
                file_number = (chunk_index - 1) * chunk_size + i
                
                # Add file entry
                file_list_msg += f"#{file_number}. {file_info['name']}\n"
                file_list_msg += f"   📏 Size: {file_size}\n"
                file_list_msg += f"   📅 Modified: {file_info['modification_time']}\n"
                file_list_msg += f"   📍 Source: {file_info['source']}\n"
                
                # Check if file is in read-only location
                file_dir = os.path.dirname(file_info['path'])
                is_read_only = not os.access(file_dir, os.W_OK)
                
                # Add warnings
                if file_info['is_too_large']:
                    file_list_msg += f"   ⚠️ TOO LARGE FOR TELEGRAM (max 2GB)\n"
                elif DELETE_AFTER_UPLOAD:
                    if is_read_only:
                        file_list_msg += f"   🗑️ READ-ONLY (will be kept)\n"
                    else:
                        file_list_msg += f"   🗑️ Will be deleted after upload\n"
                
                file_list_msg += "\n"
            
            # Add chunk info
            if len(file_chunks) > 1:
                file_list_msg += f"--- Part {chunk_index} of {len(file_chunks)} ---\n"
            
            self.logger.info(f"📤 Sending file list chunk {chunk_index}/{len(file_chunks)}")
            await self.telegram_uploader.send_message(file_list_msg)
            
            # Small delay between messages to avoid rate limiting
            if chunk_index < len(file_chunks):
                await asyncio.sleep(2)
    
    async def delete_files_after_upload(self, files: list) -> int:
        """Delete files after successful upload if configured"""
        if not DELETE_AFTER_UPLOAD:
            self.logger.info("🗑️ Auto-delete is disabled, skipping file deletion")
            return 0
        
        self.logger.info(f"🗑️ Starting deletion of {len(files)} files after upload")
        deleted_count = 0
        read_only_filesystems = set()
        failed_deletions = 0
        
        for file_info in files:
            result = self.cleanup_manager.delete_file_after_upload(file_info)
            if result is True:
                deleted_count += 1
                self.logger.info(f"🗑️ ✅ Deleted: {file_info['name']}")
            elif result is False:
                failed_deletions += 1
                # Check if it's a read-only filesystem
                file_dir = os.path.dirname(file_info['path'])
                if not os.access(file_dir, os.W_OK):
                    read_only_filesystems.add(file_dir)
                self.logger.warning(f"🗑️ ❌ Failed to delete: {file_info['name']}")
        
        self.logger.info(f"🗑️ Deletion completed: {deleted_count}/{len(files)} files deleted")
        
        # Log detailed statistics
        if deleted_count > 0:
            self.logger.info(f"🗑️ Successfully deleted {deleted_count} files")
        
        if read_only_filesystems:
            self.logger.warning(f"🗑️ Read-only filesystems prevented deletion: {list(read_only_filesystems)}")
        
        if failed_deletions > 0 and not read_only_filesystems:
            self.logger.error(f"🗑️ {failed_deletions} files failed to delete (not due to read-only FS)")
        
        # Send notification about read-only filesystems
        if read_only_filesystems:
            await self.telegram_uploader.send_message(
                f"⚠️ READ-ONLY FILESYSTEMS DETECTED\n\n"
                f"Files could not be deleted from these locations:\n" +
                "\n".join([f"• {path}" for path in sorted(read_only_filesystems)]) +
                f"\n\n📁 Files kept: {failed_deletions}\n"
                f"✅ Files deleted: {deleted_count}\n"
                f"📊 Total processed: {len(files)}"
            )
        
        return deleted_count
    
    async def send_completion_message(self, upload_results: dict):
        """Send completion message with upload statistics"""
        total_uploaded_display = format_size(upload_results['total_uploaded_bytes'])
        
        # Add deletion statistics
        deletion_stats = ""
        if DELETE_AFTER_UPLOAD:
            deleted_count = upload_results.get('deleted_files', 0)
            total_files = upload_results['total']
            deletion_stats = f"🗑️ Deleted: {deleted_count}/{total_files}\n"
            
            # Add note if not all files were deleted
            if deleted_count < total_files and upload_results['successful'] > 0:
                deletion_stats += f"📝 Note: Some files were kept (read-only sources)\n"
        
        message = (
            f"🎉 UPLOAD COMPLETED!\n\n"
            f"📊 Final Statistics:\n"
            f"✅ Successful: {upload_results['successful']}/{upload_results['total']}\n"
            f"❌ Failed: {upload_results['failed']}\n"
            f"{deletion_stats}"
            f"💾 Data Sent: {total_uploaded_display}\n"
            f"⏰ Completion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        # Add failed files if any
        if upload_results['failed_uploads']:
            failed_msg = "\n\n❌ Failed Files:\n"
            for i, failed in enumerate(upload_results['failed_uploads'][:10], 1):
                failed_size = format_size(failed['size_bytes'])
                failed_msg += f"{i}. {failed['name']} ({failed_size}) - {failed['source']}\n"
            
            if len(upload_results['failed_uploads']) > 10:
                failed_msg += f"... and {len(upload_results['failed_uploads']) - 10} more\n"
            
            message += failed_msg
        
        await self.telegram_uploader.send_message(message)
        
        # Send error notification if there were failures
        if upload_results['failed'] > 0:
            error_msg = (
                f"⚠️ UPLOAD COMPLETED WITH ERRORS\n\n"
                f"Failed: {upload_results['failed']}/{upload_results['total']} files\n"
                f"Check logs for details"
            )
            await self.telegram_uploader.send_error_notification(error_msg)
    
    async def run(self):
        """Main execution function"""
        session_start = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info(f"BACKUP BOT SESSION STARTED: {session_start.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 80)
        
        try:
            # Step 1: Load and prepare sources
            self.logger.info("📋 STEP 1: Loading sources...")
            if not self.load_and_prepare_sources():
                error_msg = "❌ Failed to load sources. Exiting."
                self.logger.error(error_msg)
                # Initialize Telegram client just to send error notification
                if await self.telegram_uploader.initialize():
                    await self.telegram_uploader.send_error_notification("Failed to load sources. Check sources.txt file.")
                    await self.telegram_uploader.disconnect()
                return False
            
            # Step 2: Initialize Telegram client
            self.logger.info("📱 STEP 2: Initializing Telegram client...")
            if not await self.telegram_uploader.initialize():
                self.logger.error("❌ Failed to initialize Telegram client. Exiting.")
                return False
            
            # Step 3: Discover files
            self.logger.info("🔍 STEP 3: Discovering files...")
            files = self.file_processor.discover_files_from_sources(self.sources)
            
            if not files:
                self.logger.warning("⚠️ No files found for upload")
                await self.telegram_uploader.send_message("⚠️ No files found for upload")
                await self.telegram_uploader.disconnect()
                return True
            
            # Generate and log file summary
            files_summary = self.file_processor.get_files_summary(files)
            self.file_processor.log_detailed_file_info(files)
            
            # Step 4: Send startup message with full file list
            self.logger.info("📤 STEP 4: Sending startup message with file list...")
            await self.send_startup_message(files_summary, files)
            
            # Step 5: Upload files
            self.logger.info("⬆️ STEP 5: Starting file uploads...")
            upload_results = await self.telegram_uploader.send_files_batch(files)
            
            # Step 5.1: Delete files after upload if configured
            self.logger.info("🗑️ STEP 5.1: Processing file deletion after upload...")
            deleted_files_count = await self.delete_files_after_upload(files)
            upload_results['deleted_files'] = deleted_files_count
            
            # Step 6: Send completion message
            self.logger.info("📊 STEP 6: Sending completion message...")
            await self.send_completion_message(upload_results)
            
            # Calculate session duration
            session_duration = datetime.now() - session_start
            duration_str = str(session_duration).split('.')[0]  # Remove microseconds
            self.logger.info(f"✅ Session completed in {duration_str}")
            
            return upload_results['failed'] == 0
            
        except Exception as e:
            error_msg = f"❌ Fatal error in main execution: {e}"
            self.logger.error(error_msg)
            try:
                await self.telegram_uploader.send_error_notification(f"Fatal error: {str(e)}")
            except Exception as send_error:
                self.logger.error(f"Failed to send error notification: {send_error}")
            return False
        
        finally:
            # Cleanup
            self.logger.info("🧹 Cleaning up...")
            self.mount_manager.cleanup_mounts()
            await self.telegram_uploader.disconnect()
            
            self.logger.info("=" * 80)
            self.logger.info(f"BACKUP BOT SESSION ENDED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("=" * 80)

async def main():
    """Main entry point"""
    try:
        # Setup logging
        logger = setup_logging()
        
        print("=" * 70)
        print("TELEGRAM BACKUP BOT - MULTI-SOURCE VERSION")
        print("=" * 70)
        print(f"Sources file: {SOURCES_FILE}")
        print(f"Log file: /var/log/telegram_backup_bot.log")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # Create and run bot
        bot = BackupBot()
        success = await bot.run()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
        print("\n⏹️ Script stopped by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled error in main: {e}")
        print(f"❌ Critical error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
