#!/usr/bin/env python3
"""
File discovery and processing
"""
import os
import glob
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from config import FILE_EXTENSIONS, MAX_FILE_SIZE, format_size

logger = logging.getLogger('BackupBot.file_processor')

class FileProcessor:
    """Handles file discovery and information gathering"""
    
    def __init__(self):
        self.found_files = []
        self.sources_stats = {}
    
    def get_file_info(self, file_path: str, source: str = "Unknown") -> Dict:
        """Get detailed information about a file"""
        try:
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            file_size_gb = file_size / (1024 * 1024 * 1024)
            
            # Get timestamps
            try:
                creation_time = datetime.fromtimestamp(os.path.getctime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
            except:
                creation_time = "Unknown"
            
            try:
                modification_time = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
            except:
                modification_time = "Unknown"
            
            return {
                'name': os.path.basename(file_path),
                'path': file_path,
                'source': source,
                'size_bytes': file_size,
                'size_mb': file_size_mb,
                'size_gb': file_size_gb,
                'creation_time': creation_time,
                'modification_time': modification_time,
                'is_too_large': file_size > MAX_FILE_SIZE
            }
            
        except Exception as e:
            logger.error(f"Error getting file info for {file_path}: {e}")
            return None
    
    def find_files_in_source(self, source_path: str, source_name: str = None) -> List[Dict]:
        """Find all matching files in a source directory"""
        if source_name is None:
            source_name = source_path
        
        logger.info(f"Searching for files in: {source_path}")
        found_files = []
        
        try:
            for extension in FILE_EXTENSIONS:
                # Build search pattern
                if source_path.startswith('\\\\'):
                    # Windows network path
                    search_pattern = os.path.join(source_path, "**", extension)
                else:
                    # Linux path
                    search_pattern = os.path.join(source_path, "**", extension)
                
                # Find files
                try:
                    files = glob.glob(search_pattern, recursive=True)
                    logger.debug(f"Found {len(files)} files with pattern {extension} in {source_path}")
                    
                    for file_path in files:
                        if os.path.isfile(file_path):  # Only files, not directories
                            file_info = self.get_file_info(file_path, source_name)
                            if file_info:
                                found_files.append(file_info)
                                
                except Exception as e:
                    logger.warning(f"Error searching with pattern {extension} in {source_path}: {e}")
                    continue
            
            # Sort by size (smallest first)
            found_files.sort(key=lambda x: x['size_bytes'])
            
            logger.info(f"Found {len(found_files)} total files in {source_path}")
            return found_files
            
        except Exception as e:
            logger.error(f"Error searching in source {source_path}: {e}")
            return []
    
    def discover_files_from_sources(self, sources: List[Tuple[str, str]]) -> List[Dict]:
        """
        Discover files from all sources
        sources: list of (source_name, source_path) tuples
        """
        logger.info(f"Starting file discovery from {len(sources)} sources")
        
        all_files = []
        self.sources_stats = {}
        
        for source_name, source_path in sources:
            try:
                logger.info(f"Processing source: {source_name} -> {source_path}")
                
                source_files = self.find_files_in_source(source_path, source_name)
                all_files.extend(source_files)
                
                # Update statistics
                source_size_bytes = sum(f['size_bytes'] for f in source_files)
                self.sources_stats[source_name] = {
                    'file_count': len(source_files),
                    'total_size_bytes': source_size_bytes,
                    'total_size_gb': source_size_bytes / (1024**3)
                }
                
                logger.info(f"Source {source_name}: {len(source_files)} files, {format_size(source_size_bytes)}")
                
            except Exception as e:
                logger.error(f"Error processing source {source_name}: {e}")
                continue
        
        logger.info(f"File discovery completed: {len(all_files)} files found across {len(sources)} sources")
        return all_files
    
    def get_files_summary(self, files: List[Dict]) -> Dict:
        """Generate summary statistics for files"""
        if not files:
            return {
                'total_files': 0,
                'total_size_bytes': 0,
                'total_size_gb': 0,
                'sources_count': 0,
                'large_files': 0,
                'too_large_files': 0
            }
        
        total_size_bytes = sum(f['size_bytes'] for f in files)
        total_size_gb = total_size_bytes / (1024**3)
        sources = set(f['source'] for f in files)
        large_files = len([f for f in files if f['size_mb'] > 100])
        too_large_files = len([f for f in files if f['is_too_large']])
        
        return {
            'total_files': len(files),
            'total_size_bytes': total_size_bytes,
            'total_size_gb': total_size_gb,
            'sources_count': len(sources),
            'large_files': large_files,
            'too_large_files': too_large_files,
            'sources_stats': self.sources_stats
        }
    
    def log_detailed_file_info(self, files: List[Dict]):
        """Log detailed information about found files"""
        if not files:
            logger.info("No files found for processing")
            return
        
        logger.info("=" * 80)
        logger.info("DETAILED FILE INFORMATION")
        logger.info("=" * 80)
        
        # Group files by source
        files_by_source = {}
        for file_info in files:
            source = file_info['source']
            if source not in files_by_source:
                files_by_source[source] = []
            files_by_source[source].append(file_info)
        
        total_size_bytes = 0
        
        for source, source_files in files_by_source.items():
            logger.info(f"SOURCE: {source}")
            logger.info("-" * 60)
            
            source_size_bytes = 0
            for i, file_info in enumerate(source_files, 1):
                source_size_bytes += file_info['size_bytes']
                total_size_bytes += file_info['size_bytes']
                
                size_indicator = " ⚠️ TOO LARGE" if file_info['is_too_large'] else ""
                
                logger.info(f"  {i:3d}. {file_info['name']}")
                logger.info(f"       Size: {format_size(file_info['size_bytes'])}{size_indicator}")
                logger.info(f"       Modified: {file_info['modification_time']}")
            
            logger.info(f"  Source total: {len(source_files)} files, {format_size(source_size_bytes)}")
            logger.info("")
        
        logger.info(f"GRAND TOTAL: {len(files)} files, {format_size(total_size_bytes)}")
        
        # Log files that are too large
        too_large_files = [f for f in files if f['is_too_large']]
        if too_large_files:
            logger.info("")
            logger.info("⚠️  FILES TOO LARGE FOR UPLOAD:")
            for file_info in too_large_files:
                logger.info(f"  - {file_info['name']} ({format_size(file_info['size_bytes'])})")
        
        logger.info("=" * 80)
