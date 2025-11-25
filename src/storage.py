"""
Storage module for PEC Archiver.
Handles saving .eml files and creating folder structure.
"""

from __future__ import annotations

import os
import re
import logging
from datetime import datetime
from email.message import Message
from typing import Optional

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Storage operation error."""
    pass


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename by removing/replacing invalid characters.
    
    Args:
        filename: Original filename
    
    Returns:
        Sanitized filename safe for filesystem
    """
    # Remove or replace invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove control characters
    filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)
    # Limit length
    if len(filename) > 200:
        filename = filename[:200]
    return filename


def sanitize_folder_name(folder: str) -> str:
    """
    Sanitize IMAP folder name for use as directory name.
    
    Args:
        folder: IMAP folder name
    
    Returns:
        Sanitized folder name safe for filesystem
    """
    # Replace spaces with underscores
    folder = folder.replace(' ', '_')
    # Remove invalid characters
    folder = re.sub(r'[<>:"/\\|?*]', '_', folder)
    return folder


class Storage:
    """
    Storage handler for PEC archive.
    Creates directory structure and saves .eml files.
    """
    
    def __init__(self, base_path: str):
        """
        Initialize storage handler.
        
        Args:
            base_path: Base path for archive storage
        """
        self.base_path = base_path
    
    def get_account_path(self, account: str, date: datetime) -> str:
        """
        Get the archive path for an account and date.
        
        Path format: base_path/account/YYYY/YYYY-MM-DD/
        
        Args:
            account: Account username (email)
            date: Archive date
        
        Returns:
            Full path to account's date directory
        """
        # Sanitize account name (use part before @)
        account_name = account.split('@')[0]
        account_name = sanitize_filename(account_name)
        
        year = date.strftime('%Y')
        date_str = date.strftime('%Y-%m-%d')
        
        return os.path.join(self.base_path, account_name, year, date_str)
    
    def get_folder_path(self, account: str, date: datetime, folder: str) -> str:
        """
        Get the path for a specific folder within account's date directory.
        
        Args:
            account: Account username
            date: Archive date
            folder: IMAP folder name
        
        Returns:
            Full path to folder directory
        """
        account_path = self.get_account_path(account, date)
        folder_name = sanitize_folder_name(folder)
        return os.path.join(account_path, folder_name)
    
    def create_directory_structure(
        self,
        account: str,
        date: datetime,
        folders: list[str]
    ) -> str:
        """
        Create the complete directory structure for an account.
        
        Args:
            account: Account username
            date: Archive date
            folders: List of IMAP folder names
        
        Returns:
            Path to account's date directory
        
        Raises:
            StorageError: If directory creation fails
        """
        account_path = self.get_account_path(account, date)
        
        try:
            os.makedirs(account_path, exist_ok=True)
            logger.debug(f"Created directory: {account_path}")
            
            for folder in folders:
                folder_path = os.path.join(
                    account_path,
                    sanitize_folder_name(folder)
                )
                os.makedirs(folder_path, exist_ok=True)
                logger.debug(f"Created directory: {folder_path}")
            
            return account_path
        except OSError as e:
            raise StorageError(f"Failed to create directory structure: {e}")
    
    def save_eml(
        self,
        account: str,
        date: datetime,
        folder: str,
        uid: str,
        message: Message,
        raw_email: bytes
    ) -> str:
        """
        Save a message as .eml file.
        
        Args:
            account: Account username
            date: Archive date
            folder: IMAP folder name
            uid: Message UID
            message: Parsed email message
            raw_email: Raw email bytes
        
        Returns:
            Path to saved .eml file
        
        Raises:
            StorageError: If save fails
        """
        folder_path = self.get_folder_path(account, date, folder)
        
        # Create filename from subject or UID
        subject = message.get('Subject', 'no_subject')
        if subject:
            subject = sanitize_filename(str(subject))[:50]
        else:
            subject = 'no_subject'
        
        filename = f"{uid}_{subject}.eml"
        filepath = os.path.join(folder_path, filename)
        
        try:
            os.makedirs(folder_path, exist_ok=True)
            with open(filepath, 'wb') as f:
                f.write(raw_email)
            logger.debug(f"Saved message to: {filepath}")
            return filepath
        except OSError as e:
            raise StorageError(f"Failed to save .eml file: {e}")
    
    def get_saved_messages(
        self,
        account: str,
        date: datetime,
        folder: str
    ) -> list[str]:
        """
        Get list of saved .eml files for a folder.
        
        Args:
            account: Account username
            date: Archive date
            folder: IMAP folder name
        
        Returns:
            List of .eml file paths
        """
        folder_path = self.get_folder_path(account, date, folder)
        
        if not os.path.exists(folder_path):
            return []
        
        return [
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if f.endswith('.eml')
        ]
    
    def get_all_saved_messages(
        self,
        account: str,
        date: datetime,
        folders: list[str]
    ) -> dict[str, list[str]]:
        """
        Get all saved .eml files for all folders.
        
        Args:
            account: Account username
            date: Archive date
            folders: List of IMAP folder names
        
        Returns:
            Dictionary mapping folder names to lists of .eml file paths
        """
        result = {}
        for folder in folders:
            result[folder] = self.get_saved_messages(account, date, folder)
        return result
