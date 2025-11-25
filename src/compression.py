"""
Compression module for PEC Archiver.
Creates .tar.gz archives and generates SHA256 digest.
"""

from __future__ import annotations

import os
import tarfile
import hashlib
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CompressionError(Exception):
    """Compression operation error."""
    pass


def create_archive(
    source_path: str,
    account_name: str,
    date: datetime,
    exclude_patterns: list[str] = None
) -> str:
    """
    Create a .tar.gz archive of the account's date directory.
    
    Args:
        source_path: Path to account's date directory
        account_name: Account name for archive filename
        date: Archive date
        exclude_patterns: Patterns to exclude from archive
    
    Returns:
        Path to created archive file
    
    Raises:
        CompressionError: If archive creation fails
    """
    if exclude_patterns is None:
        exclude_patterns = []
    
    date_str = date.strftime('%Y-%m-%d')
    archive_name = f"archive-{account_name}-{date_str}.tar.gz"
    archive_path = os.path.join(source_path, archive_name)
    
    def should_exclude(tarinfo):
        """Filter function to exclude certain files."""
        name = tarinfo.name
        # Exclude the archive file itself
        if name.endswith('.tar.gz'):
            return None
        # Exclude digest files
        if name.endswith('.sha256'):
            return None
        # Exclude summary.json as it's created after
        if name.endswith('summary.json'):
            return None
        # Check custom exclusion patterns
        for pattern in exclude_patterns:
            if pattern in name:
                return None
        return tarinfo
    
    try:
        with tarfile.open(archive_path, 'w:gz') as tar:
            # Add all files in source_path
            for item in os.listdir(source_path):
                item_path = os.path.join(source_path, item)
                tar.add(
                    item_path,
                    arcname=item,
                    filter=should_exclude
                )
        
        archive_size = os.path.getsize(archive_path)
        logger.info(
            f"Created archive: {archive_path} "
            f"({archive_size / (1024*1024):.2f} MB)"
        )
        return archive_path
    except Exception as e:
        raise CompressionError(f"Failed to create archive: {e}")


def calculate_sha256(filepath: str) -> str:
    """
    Calculate SHA256 hash of a file.
    
    Args:
        filepath: Path to file
    
    Returns:
        SHA256 hash as hexadecimal string
    """
    sha256_hash = hashlib.sha256()
    
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()


def create_digest(archive_path: str) -> str:
    """
    Create SHA256 digest file for an archive.
    
    Args:
        archive_path: Path to archive file
    
    Returns:
        Path to digest file
    
    Raises:
        CompressionError: If digest creation fails
    """
    try:
        digest = calculate_sha256(archive_path)
        archive_name = os.path.basename(archive_path)
        digest_path = os.path.join(
            os.path.dirname(archive_path),
            'digest.sha256'
        )
        
        with open(digest_path, 'w', encoding='utf-8') as f:
            f.write(f"{digest}  {archive_name}\n")
        
        logger.info(f"Created digest: {digest_path}")
        return digest_path
    except Exception as e:
        raise CompressionError(f"Failed to create digest: {e}")


def verify_archive(archive_path: str, digest_path: str) -> bool:
    """
    Verify archive integrity using digest file.
    
    Args:
        archive_path: Path to archive file
        digest_path: Path to digest file
    
    Returns:
        True if verification succeeds, False otherwise
    """
    try:
        # Read expected digest
        with open(digest_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            expected_digest = content.split()[0]
        
        # Calculate actual digest
        actual_digest = calculate_sha256(archive_path)
        
        if actual_digest == expected_digest:
            logger.info("Archive verification: OK")
            return True
        else:
            logger.error(
                f"Archive verification: FAILED "
                f"(expected: {expected_digest}, got: {actual_digest})"
            )
            return False
    except Exception as e:
        logger.error(f"Archive verification failed: {e}")
        return False
