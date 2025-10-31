#!/usr/bin/env python3
"""
Network mount management for Windows shares
"""
import os
import subprocess
import logging
import tempfile
import time
from typing import List, Tuple, Optional

logger = logging.getLogger('BackupBot.network_mount')

class NetworkMountManager:
    """Manages mounting and unmounting of network shares"""
    
    def __init__(self):
        self.active_mounts = {}
        self.mount_base = "/mnt/telegram_backup"
        
        # Create mount base directory
        os.makedirs(self.mount_base, exist_ok=True)
    
    def parse_source_line(self, line: str) -> Tuple[str, str, str, str]:
        """
        Parse source line: path|username|password|mount_point
        Returns: (path, username, password, mount_point)
        """
        parts = line.split('|')
        path = parts[0].strip() if len(parts) > 0 else ""
        username = parts[1].strip() if len(parts) > 1 else ""
        password = parts[2].strip() if len(parts) > 2 else ""
        mount_point = parts[3].strip() if len(parts) > 3 else ""
        
        return path, username, password, mount_point
    
    def is_windows_network_path(self, path: str) -> bool:
        """Check if path is a Windows network path"""
        return path.startswith('\\\\') or path.startswith('//')
    
    def is_local_path(self, path: str) -> bool:
        """Check if path is a local Linux path"""
        return path.startswith('/') and not path.startswith('//')
    
    def mount_windows_share(self, windows_path: str, username: str = "", password: str = "", mount_point: str = "") -> Optional[str]:
        """Mount Windows network share"""
        try:
            # Generate mount point if not provided
            if not mount_point:
                # Create unique mount point name from windows path
                mount_name = windows_path.replace('\\\\', '').replace('\\', '_').replace('/', '_').replace(':', '')
                mount_point = os.path.join(self.mount_base, mount_name)
            
            # Create mount directory
            os.makedirs(mount_point, exist_ok=True)
            
            # Check if already mounted (by our script or externally)
            if self.is_mounted(mount_point):
                logger.info(f"Share already mounted at {mount_point}")
                # Check if it's mounted by us
                if mount_point not in self.active_mounts:
                    logger.warning(f"Share was mounted externally, will try to unmount it first")
                    if not self.unmount_share(mount_point):
                        logger.error(f"Failed to unmount externally mounted share")
                        return None
                else:
                    return mount_point
            
            # Prepare mount command
            # Convert Windows path to smb path
            smb_path = windows_path.replace('\\', '/')
            if not smb_path.startswith('//'):
                smb_path = '//' + smb_path.lstrip('/')
            
            # Build credentials
            credentials = []
            if username:
                credentials.extend(['-o', f'username={username}'])
                if password:
                    # Use credentials file for security
                    cred_file = self.create_credentials_file(username, password)
                    credentials.extend(['-o', f'credentials={cred_file}'])
            else:
                # Guest access
                credentials.extend(['-o', 'guest'])
            
            # Mount command - ИЗМЕНЕНО: ro -> rw для разрешения удаления файлов
            mount_options = [
                'rw',           # read-write вместо read-only
                'vers=3.0',     # SMB version
                'cache=strict', # кэширование
                'uid=' + str(os.getuid()),  # текущий пользователь
                'forceuid',     # принудительно использовать указанный uid
                'gid=' + str(os.getgid()),  # текущая группа
                'forcegid',     # принудительно использовать указанный gid
                'file_mode=0644',
                'dir_mode=0755'
            ]
            
            mount_cmd = ['mount', '-t', 'cifs', smb_path, mount_point] + credentials + ['-o', ','.join(mount_options)]
            
            logger.info(f"Mounting {windows_path} to {mount_point}")
            logger.info(f"Mount command: {' '.join(mount_cmd)}")
            result = subprocess.run(mount_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"✅ Successfully mounted {windows_path} to {mount_point}")
                self.active_mounts[mount_point] = {
                    'windows_path': windows_path,
                    'username': username,
                    'has_password': bool(password),
                    'mounted_by_script': True,
                    'mount_time': time.time()
                }
                
                # Проверяем права записи после монтирования
                if self.test_write_permission(mount_point):
                    logger.info(f"✅ Write permissions confirmed for {mount_point}")
                else:
                    logger.warning(f"⚠️ Write permissions issue detected for {mount_point}")
                    
                return mount_point
            else:
                logger.error(f"❌ Failed to mount {windows_path}: {result.stderr}")
                # Попробуем альтернативные варианты версий SMB
                return self.try_alternative_mount_versions(smb_path, mount_point, credentials, windows_path)
                
        except Exception as e:
            logger.error(f"Error mounting {windows_path}: {e}")
            return None
    
    def try_alternative_mount_versions(self, smb_path: str, mount_point: str, credentials: list, windows_path: str) -> Optional[str]:
        """Try alternative SMB versions if default fails"""
        smb_versions = ['3.0', '2.1', '2.0', '1.0']
        
        for version in smb_versions:
            try:
                logger.info(f"Trying SMB version {version} for {windows_path}")
                
                mount_options = [
                    'rw',
                    f'vers={version}',
                    'cache=strict',
                    'uid=' + str(os.getuid()),
                    'forceuid',
                    'gid=' + str(os.getgid()),
                    'forcegid',
                    'file_mode=0644',
                    'dir_mode=0755'
                ]
                
                mount_cmd = ['mount', '-t', 'cifs', smb_path, mount_point] + credentials + ['-o', ','.join(mount_options)]
                
                result = subprocess.run(mount_cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    logger.info(f"✅ Successfully mounted {windows_path} with SMB {version}")
                    self.active_mounts[mount_point] = {
                        'windows_path': windows_path,
                        'username': credentials[1].split('=')[1] if credentials else '',
                        'has_password': len(credentials) > 2,
                        'mounted_by_script': True,
                        'mount_time': time.time()
                    }
                    return mount_point
                    
            except Exception as e:
                logger.warning(f"Failed to mount with SMB {version}: {e}")
                continue
        
        logger.error(f"❌ All SMB versions failed for {windows_path}")
        return None
    
    def test_write_permission(self, mount_point: str) -> bool:
        """Test if we have write permission to mounted share"""
        try:
            test_file = os.path.join(mount_point, '.write_test')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            return True
        except Exception as e:
            logger.warning(f"Write test failed for {mount_point}: {e}")
            return False
    
    def create_credentials_file(self, username: str, password: str) -> str:
        """Create temporary credentials file for CIFS mount"""
        try:
            fd, cred_file = tempfile.mkstemp(prefix='smb_cred_', text=True)
            with os.fdopen(fd, 'w') as f:
                f.write(f"username={username}\n")
                f.write(f"password={password}\n")
                f.write(f"domain=WORKGROUP\n")
            return cred_file
        except Exception as e:
            logger.error(f"Error creating credentials file: {e}")
            raise
    
    def is_mounted(self, mount_point: str) -> bool:
        """Check if directory is already mounted"""
        try:
            result = subprocess.run(['mountpoint', '-q', mount_point], capture_output=True)
            return result.returncode == 0
        except Exception:
            # Fallback: check /proc/mounts
            try:
                with open('/proc/mounts', 'r') as f:
                    mounts = f.read()
                return mount_point in mounts
            except:
                return False
    
    def prepare_source(self, source_line: str) -> Optional[str]:
        """
        Prepare source for access - mount if necessary
        Returns accessible path
        """
        path, username, password, mount_point = self.parse_source_line(source_line)
        
        if not path:
            logger.error(f"Invalid path in source line: {source_line}")
            return None
        
        # Check if local path exists
        if self.is_local_path(path):
            if os.path.exists(path):
                logger.info(f"✅ Local path accessible: {path}")
                # Проверяем права записи для локальных путей
                if os.access(path, os.W_OK):
                    logger.info(f"✅ Write permissions confirmed for local path: {path}")
                else:
                    logger.warning(f"⚠️ No write permissions for local path: {path}")
                return path
            else:
                logger.error(f"❌ Local path not found: {path}")
                return None
        
        # Mount Windows network share
        elif self.is_windows_network_path(path):
            mounted_path = self.mount_windows_share(path, username, password, mount_point)
            if mounted_path:
                return mounted_path
            else:
                logger.error(f"❌ Failed to mount Windows share: {path}")
                return None
        
        else:
            logger.error(f"❌ Unsupported path format: {path}")
            return None
    
    def unmount_share(self, mount_point: str) -> bool:
        """Unmount network share"""
        try:
            if not self.is_mounted(mount_point):
                logger.info(f"Share not mounted at {mount_point}")
                return True
            
            logger.info(f"Unmounting {mount_point}")
            
            # Try gentle unmount first
            result = subprocess.run(['umount', mount_point], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"✅ Successfully unmounted {mount_point}")
                # Remove from active mounts
                if mount_point in self.active_mounts:
                    self.active_mounts.pop(mount_point, None)
                
                # Try to remove mount directory if empty
                try:
                    os.rmdir(mount_point)
                    logger.info(f"✅ Removed mount directory: {mount_point}")
                except OSError:
                    logger.debug(f"Mount directory not empty, keeping: {mount_point}")
                    
                return True
            else:
                logger.warning(f"Gentle unmount failed, trying lazy unmount: {result.stderr}")
                # Try lazy unmount
                result = subprocess.run(['umount', '-l', mount_point], capture_output=True, text=True)
                if result.returncode == 0:
                    logger.info(f"✅ Successfully lazy-unmounted {mount_point}")
                    if mount_point in self.active_mounts:
                        self.active_mounts.pop(mount_point, None)
                    return True
                else:
                    logger.error(f"❌ Failed to unmount {mount_point}: {result.stderr}")
                    return False
                
        except Exception as e:
            logger.error(f"Error unmounting {mount_point}: {e}")
            return False
    
    def cleanup_mounts(self):
        """Unmount all active mounts and cleanup"""
        logger.info(f"Starting mount cleanup...")
        
        # First cleanup our own mounts
        if self.active_mounts:
            logger.info(f"Cleaning up {len(self.active_mounts)} script-managed mounts...")
            
            for mount_point in list(self.active_mounts.keys()):
                logger.info(f"Unmounting script-managed mount: {mount_point}")
                self.unmount_share(mount_point)
        
        # Also check and cleanup any mounts in our mount base directory
        try:
            if os.path.exists(self.mount_base):
                for item in os.listdir(self.mount_base):
                    mount_point = os.path.join(self.mount_base, item)
                    if os.path.isdir(mount_point) and self.is_mounted(mount_point):
                        logger.info(f"Found orphaned mount at {mount_point}, unmounting...")
                        self.unmount_share(mount_point)
        except Exception as e:
            logger.error(f"Error cleaning up orphaned mounts: {e}")
        
        logger.info("Mount cleanup completed")
    
    def get_mount_info(self, mount_point: str) -> dict:
        """Get information about mounted share"""
        try:
            if not self.is_mounted(mount_point):
                return {}
            
            result = subprocess.run(['mount'], capture_output=True, text=True)
            lines = result.stdout.split('\n')
            
            for line in lines:
                if mount_point in line:
                    return {
                        'mounted': True,
                        'details': line.strip(),
                        'managed_by_script': mount_point in self.active_mounts
                    }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting mount info for {mount_point}: {e}")
            return {}
    
    def force_unmount(self, mount_point: str) -> bool:
        """Force unmount share (use when regular unmount fails)"""
        try:
            if not self.is_mounted(mount_point):
                return True
            
            logger.warning(f"Force unmounting {mount_point}")
            
            # Try force lazy unmount
            result = subprocess.run(['umount', '-f', '-l', mount_point], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"✅ Successfully force-unmounted {mount_point}")
                if mount_point in self.active_mounts:
                    self.active_mounts.pop(mount_point, None)
                return True
            else:
                logger.error(f"❌ Failed to force-unmount {mount_point}: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error force-unmounting {mount_point}: {e}")
            return False
    
    def __del__(self):
        """Destructor to ensure cleanup"""
        self.cleanup_mounts()
