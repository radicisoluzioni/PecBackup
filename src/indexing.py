"""
Indexing module for PEC Archiver.
Generates index.csv and index.json files.
"""

from __future__ import annotations

import os
import csv
import json
import email
import logging
from datetime import datetime
from email.message import Message
from email.utils import parsedate_to_datetime
from email.header import decode_header
from typing import Optional

logger = logging.getLogger(__name__)


def decode_email_header(header_value: Optional[str]) -> str:
    """
    Decode email header value handling various encodings.
    
    Args:
        header_value: Raw header value
    
    Returns:
        Decoded string
    """
    if not header_value:
        return ''
    
    try:
        decoded_parts = []
        for part, encoding in decode_header(header_value):
            if isinstance(part, bytes):
                if encoding:
                    decoded_parts.append(part.decode(encoding, errors='replace'))
                else:
                    decoded_parts.append(part.decode('utf-8', errors='replace'))
            else:
                decoded_parts.append(str(part))
        return ''.join(decoded_parts)
    except Exception:
        return str(header_value)


def parse_email_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse email date header to datetime.
    
    Args:
        date_str: Email date header value
    
    Returns:
        datetime object or None if parsing fails
    """
    if not date_str:
        return None
    
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        return None


def extract_message_info(
    message: Message,
    uid: str,
    folder: str,
    filepath: str,
    is_unread: bool = False
) -> dict:
    """
    Extract metadata from an email message.
    
    Args:
        message: Parsed email message
        uid: Message UID
        folder: IMAP folder name
        filepath: Path to saved .eml file
        is_unread: Whether the message is unread
    
    Returns:
        Dictionary with message metadata
    """
    date = parse_email_date(message.get('Date'))
    
    return {
        'uid': uid,
        'folder': folder,
        'filename': os.path.basename(filepath),
        'filepath': filepath,
        'subject': decode_email_header(message.get('Subject')),
        'from': decode_email_header(message.get('From')),
        'to': decode_email_header(message.get('To')),
        'cc': decode_email_header(message.get('Cc')),
        'date': date.isoformat() if date else '',
        'message_id': message.get('Message-ID', ''),
        'size': os.path.getsize(filepath) if os.path.exists(filepath) else 0,
        'is_unread': is_unread
    }


def load_message_from_file(filepath: str) -> Optional[Message]:
    """
    Load and parse an email message from .eml file.
    
    Args:
        filepath: Path to .eml file
    
    Returns:
        Parsed Message object or None if parsing fails
    """
    try:
        with open(filepath, 'rb') as f:
            return email.message_from_bytes(f.read())
    except Exception as e:
        logger.error(f"Failed to load message from {filepath}: {e}")
        return None


class Indexer:
    """
    Index generator for PEC archive.
    Creates index.csv and index.json files.
    """
    
    def __init__(self, account_path: str):
        """
        Initialize indexer.
        
        Args:
            account_path: Path to account's date directory
        """
        self.account_path = account_path
        self.messages = []
    
    def add_message(
        self,
        message: Message,
        uid: str,
        folder: str,
        filepath: str,
        is_unread: bool = False
    ) -> None:
        """
        Add a message to the index.
        
        Args:
            message: Parsed email message
            uid: Message UID
            folder: IMAP folder name
            filepath: Path to saved .eml file
            is_unread: Whether the message is unread
        """
        info = extract_message_info(message, uid, folder, filepath, is_unread)
        self.messages.append(info)
    
    def load_messages_from_files(
        self,
        folder_messages: dict[str, list[str]]
    ) -> None:
        """
        Load messages from saved .eml files.
        
        Args:
            folder_messages: Dictionary mapping folder names to lists of .eml file paths
        """
        for folder, files in folder_messages.items():
            for filepath in files:
                message = load_message_from_file(filepath)
                if message:
                    # Extract UID from filename (format: uid_subject.eml)
                    filename = os.path.basename(filepath)
                    uid = filename.split('_')[0] if '_' in filename else filename
                    self.add_message(message, uid, folder, filepath)
    
    def generate_csv(self) -> str:
        """
        Generate index.csv file.
        
        Returns:
            Path to generated CSV file
        """
        csv_path = os.path.join(self.account_path, 'index.csv')
        
        fieldnames = [
            'uid', 'folder', 'filename', 'subject', 'from', 'to',
            'cc', 'date', 'message_id', 'size', 'is_unread'
        ]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for msg in self.messages:
                writer.writerow(msg)
        
        logger.info(f"Generated index.csv with {len(self.messages)} entries")
        return csv_path
    
    def generate_json(self) -> str:
        """
        Generate index.json file.
        
        Returns:
            Path to generated JSON file
        """
        json_path = os.path.join(self.account_path, 'index.json')
        
        # Create relative paths for JSON
        index_data = []
        for msg in self.messages:
            msg_copy = msg.copy()
            # Make filepath relative to account_path
            if msg_copy.get('filepath'):
                msg_copy['filepath'] = os.path.relpath(
                    msg_copy['filepath'],
                    self.account_path
                )
            index_data.append(msg_copy)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Generated index.json with {len(self.messages)} entries")
        return json_path
    
    def generate_all(self) -> tuple[str, str]:
        """
        Generate both index.csv and index.json files.
        
        Returns:
            Tuple of (csv_path, json_path)
        """
        csv_path = self.generate_csv()
        json_path = self.generate_json()
        return csv_path, json_path
    
    def get_stats(self) -> dict:
        """
        Get statistics about indexed messages.
        
        Returns:
            Dictionary with statistics
        """
        folders = {}
        total_size = 0
        
        for msg in self.messages:
            folder = msg['folder']
            folders[folder] = folders.get(folder, 0) + 1
            total_size += msg.get('size', 0)
        
        return {
            'total_messages': len(self.messages),
            'folders': folders,
            'total_size': total_size
        }
