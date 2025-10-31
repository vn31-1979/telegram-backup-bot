#!/usr/bin/env python3
"""
Cleanup manager for removing files after upload and disk space management
"""
import os
import csv
import logging
import shutil
from datetime import datetime, timedelta
from typing import List, Dict
from config import format_size, DELETE_AFTER_UPLOAD

logger = logging.getLogger('BackupBot.cleanup_manager')

class CleanupManager:
    """Manages file cleanup after upload and disk space management"""
    
    def __init__(self, upload_history_file: str = 'upload_history.csv'):
        self.upload_history_file = upload_history_file
        self.upload_history = []
        self.load_upload_history()
    
    def load_upload_history(self):
        """Load upload history from CSV file"""
        if not os.path.exists(self.upload_history_file):
            logger.warning(f"Upload history file not found: {self.upload_history_file}")
            return
        
        try:
            with open(self.upload_history_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.upload_history = list(reader)
            
            logger.info(f"Loaded {len(self.upload_history)} records from upload history")
            
        except Exception as e:
            logger.error(f"Error loading upload history: {e}")
            self.upload_history = []
    
    def delete_file_after_upload(self, file_info: Dict) -> bool:
        """Delete file after successful upload if configured"""
        logger.info(f"🗑️ DELETE_AFTER_UPLOAD setting: {DELETE_AFTER_UPLOAD}")
        
        if not DELETE_AFTER_UPLOAD:
            logger.info(f"🗑️ Auto-delete is disabled, skipping deletion for: {file_info['name']}")
            return False
        
        try:
            file_path = file_info['path']
            
            logger.info(f"🗑️ Starting deletion process for: {file_path}")
            
            # Check if file exists
            if not os.path.exists(file_path):
                logger.warning(f"🗑️ File not found for deletion: {file_path}")
                return False
            
            # Check if filesystem is read-only
            try:
                # Try to create a test file to check write permissions
                test_path = os.path.join(os.path.dirname(file_path), '.write_test')
                try:
                    with open(test_path, 'w') as f:
                        f.write('test')
                    os.remove(test_path)
                    logger.info(f"🗑️ Filesystem is writable: {os.path.dirname(file_path)}")
                except (OSError, IOError) as e:
                    if 'Read-only' in str(e) or 'EROFS' in str(e):
                        logger.warning(f"🗑️ Filesystem is read-only, cannot delete files: {os.path.dirname(file_path)}")
                        logger.warning(f"🗑️ File will be kept: {file_info['name']}")
                        return False
            except Exception as test_error:
                logger.warning(f"🗑️ Could not test filesystem permissions: {test_error}")
            
            # Get file info before deletion
            file_size = file_info['size_bytes']
            file_size_display = format_size(file_size)
            
            logger.info(f"🗑️ File exists: {os.path.exists(file_path)}")
            logger.info(f"🗑️ File size: {file_size_display}")
            
            # Get file permissions info
            try:
                file_stat = os.stat(file_path)
                logger.info(f"🗑️ File permissions: {oct(file_stat.st_mode)}")
                logger.info(f"🗑️ File owner UID: {file_stat.st_uid}")
                logger.info(f"🗑️ Current process UID: {os.getuid()}")
                
                # Check if we have write permission to the directory
                dir_path = os.path.dirname(file_path)
                dir_stat = os.stat(dir_path)
                can_write = os.access(dir_path, os.W_OK)
                logger.info(f"🗑️ Directory write permission: {can_write}")
                
                if not can_write:
                    logger.warning(f"🗑️ No write permission to directory: {dir_path}")
                    logger.warning(f"🗑️ File will be kept: {file_info['name']}")
                    return False
                    
            except Exception as e:
                logger.warning(f"🗑️ Could not get file stats: {e}")
            
            # Delete the file
            try:
                os.remove(file_path)
                logger.info("🗑️ os.remove() completed")
            except OSError as e:
                if e.errno == 30:  # Read-only file system
                    logger.warning(f"🗑️ Read-only file system, cannot delete: {file_path}")
                    logger.warning(f"🗑️ File will be kept: {file_info['name']}")
                    return False
                else:
                    raise e
            
            # Verify deletion
            if os.path.exists(file_path):
                logger.error(f"❌ File still exists after deletion: {file_path}")
                return False
            else:
                logger.info(f"🗑️ ✅ Successfully deleted: {file_info['name']} ({file_size_display})")
                return True
                
        except PermissionError as e:
            logger.error(f"❌ Permission denied deleting file {file_info['name']}: {e}")
            return False
        except OSError as e:
            if e.errno == 30:  # Read-only file system
                logger.warning(f"🗑️ Read-only file system, cannot delete: {file_info['name']}")
                return False
            else:
                logger.error(f"❌ OS error deleting file {file_info['name']}: {e}")
                return False
        except Exception as e:
            logger.error(f"❌ Error deleting file {file_info['name']}: {e}")
            logger.error(f"❌ File path: {file_info['path']}")
            return False
    
    def get_successful_uploads(self) -> List[Dict]:
        """Get list of successfully uploaded files"""
        return [record for record in self.upload_history 
                if record.get('upload_success', '').lower() == 'true']
    
    def get_disk_usage(self, path: str = '/') -> Dict:
        """Get disk usage statistics for a path"""
        try:
            total, used, free = shutil.disk_usage(path)
            return {
                'total_gb': total / (1024**3),
                'used_gb': used / (1024**3),
                'free_gb': free / (1024**3),
                'usage_percent': (used / total) * 100
            }
        except Exception as e:
            logger.error(f"Error getting disk usage for {path}: {e}")
            return None
    
    def needs_cleanup(self, threshold_gb: float = 10.0, usage_percent: float = 90.0) -> bool:
        """
        Check if cleanup is needed based on disk space
        Returns True if free space < threshold_gb OR usage > usage_percent
        """
        disk_usage = self.get_disk_usage()
        if not disk_usage:
            logger.warning("Could not determine disk usage, assuming cleanup needed")
            return True
        
        logger.info(
            f"Disk usage: {disk_usage['free_gb']:.1f}GB free, "
            f"{disk_usage['usage_percent']:.1f}% used"
        )
        
        return (disk_usage['free_gb'] < threshold_gb or 
                disk_usage['usage_percent'] > usage_percent)
    
    def get_files_to_cleanup(self, target_free_gb: float = 50.0) -> List[Dict]:
        """
        Get list of files that can be cleaned up to free up space
        Returns files sorted by modification time (oldest first)
        """
        if not self.needs_cleanup():
            logger.info("No cleanup needed - sufficient disk space available")
            return []
        
        successful_uploads = self.get_successful_uploads()
        if not successful_uploads:
            logger.info("No successfully uploaded files found for cleanup")
            return []
        
        # Get current disk usage
        disk_usage = self.get_disk_usage()
        if not disk_usage:
            return []
        
        # Calculate how much space we need to free
        current_free_gb = disk_usage['free_gb']
        space_to_free_gb = max(0, target_free_gb - current_free_gb)
        
        if space_to_free_gb <= 0:
            logger.info(f"Already have {current_free_gb:.1f}GB free, target is {target_free_gb}GB")
            return []
        
        logger.info(f"Need to free {space_to_free_gb:.1f}GB of disk space")
        
        # Collect file information for cleanup candidates
        cleanup_candidates = []
        
        for record in successful_uploads:
            try:
                file_path = record['source_path']
                
                # Skip if file doesn't exist
                if not os.path.exists(file_path):
                    continue
                
                # Get file size and modification time
                file_size = os.path.getsize(file_path)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                cleanup_candidates.append({
                    'path': file_path,
                    'size_gb': file_size / (1024**3),
                    'modification_time': mod_time,
                    'upload_date': record.get('upload_date', ''),
                    'filename': record.get('filename', '')
                })
                
            except Exception as e:
                logger.warning(f"Error processing file {record.get('filename', 'unknown')}: {e}")
                continue
        
        # Sort by modification time (oldest first)
        cleanup_candidates.sort(key=lambda x: x['modification_time'])
        
        # Select files to delete until we reach target free space
        files_to_delete = []
        total_freed_gb = 0
        
        for candidate in cleanup_candidates:
            if total_freed_gb >= space_to_free_gb:
                break
            
            files_to_delete.append(candidate)
            total_freed_gb += candidate['size_gb']
        
        logger.info(
            f"Selected {len(files_to_delete)} files for cleanup, "
            f"will free {total_freed_gb:.2f}GB"
        )
        
        return files_to_delete
    
    def cleanup_files(self, files: List[Dict]) -> Dict:
        """
        Delete specified files and return cleanup results
        """
        results = {
            'total_files': len(files),
            'successful_deletions': 0,
            'failed_deletions': 0,
            'total_freed_gb': 0
        }
        
        if not files:
            return results
        
        logger.info(f"Starting cleanup of {len(files)} files")
        
        for file_info in files:
            try:
                file_path = file_info['path']
                
                # Double check file exists
                if not os.path.exists(file_path):
                    logger.warning(f"File not found, skipping: {file_path}")
                    results['failed_deletions'] += 1
                    continue
                
                # Get file size for logging
                file_size = os.path.getsize(file_path)
                file_size_gb = file_size / (1024**3)
                
                # Delete file
                os.remove(file_path)
                
                # Verify deletion
                if os.path.exists(file_path):
                    logger.error(f"File still exists after deletion: {file_path}")
                    results['failed_deletions'] += 1
                else:
                    logger.info(f"✅ Deleted: {file_info['filename']} ({file_size_gb:.3f}GB)")
                    results['successful_deletions'] += 1
                    results['total_freed_gb'] += file_size_gb
                    
            except Exception as e:
                logger.error(f"❌ Failed to delete {file_info['filename']}: {e}")
                results['failed_deletions'] += 1
        
        # Log cleanup results
        logger.info(
            f"Cleanup completed: {results['successful_deletions']}/"
            f"{results['total_files']} successful, "
            f"freed {results['total_freed_gb']:.2f}GB"
        )
        
        return results
    
    def run_cleanup(self, target_free_gb: float = 50.0) -> Dict:
        """
        Run complete cleanup process
        Returns cleanup results dictionary
        """
        logger.info("Starting disk cleanup process")
        
        # Check if cleanup is needed
        if not self.needs_cleanup():
            return {
                'cleanup_performed': False,
                'reason': 'Sufficient disk space available'
            }
        
        # Get files to cleanup
        files_to_cleanup = self.get_files_to_cleanup(target_free_gb)
        if not files_to_cleanup:
            return {
                'cleanup_performed': False,
                'reason': 'No files available for cleanup'
            }
        
        # Perform cleanup
        cleanup_results = self.cleanup_files(files_to_cleanup)
        cleanup_results['cleanup_performed'] = True
        
        # Log final disk usage
        disk_usage = self.get_disk_usage()
        if disk_usage:
            logger.info(
                f"Final disk usage: {disk_usage['free_gb']:.1f}GB free, "
                f"{disk_usage['usage_percent']:.1f}% used"
            )
            cleanup_results['final_free_gb'] = disk_usage['free_gb']
        
        return cleanup_results

def main():
    """Standalone cleanup script"""
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/var/log/backup_cleanup.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger.info("=== BACKUP CLEANUP MANAGER STARTED ===")
    
    try:
        # Create cleanup manager
        cleanup_mgr = CleanupManager()
        
        # Run cleanup
        results = cleanup_mgr.run_cleanup(target_free_gb=50.0)
        
        # Log results
        if results.get('cleanup_performed'):
            logger.info(
                f"Cleanup successful: {results['successful_deletions']} files deleted, "
                f"{results['total_freed_gb']:.2f}GB freed"
            )
        else:
            logger.info(f"No cleanup performed: {results.get('reason', 'Unknown reason')}")
        
        logger.info("=== BACKUP CLEANUP MANAGER FINISHED ===")
        
    except Exception as e:
        logger.error(f"Cleanup manager error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
