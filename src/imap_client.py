"""
IMAP client module for PEC Archiver.
Handles IMAP connections and message fetching.
"""

from __future__ import annotations

import imaplib
import email
import ssl
import time
import logging
from datetime import datetime, timedelta
from email.message import Message
from typing import Generator, Optional

logger = logging.getLogger(__name__)


class IMAPError(Exception):
    """IMAP operation error."""
    pass


class IMAPClient:
    """
    IMAP client for connecting to PEC mailboxes.
    Supports SSL/TLS connections and message fetching by date.
    """
    
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 993,
        timeout: int = 30
    ):
        """
        Initialize IMAP client.
        
        Args:
            host: IMAP server hostname
            username: Account username
            password: Account password
            port: IMAP port (default: 993 for IMAPS)
            timeout: Connection timeout in seconds
        """
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.timeout = timeout
        self.connection: Optional[imaplib.IMAP4_SSL] = None
    
    def connect(self) -> None:
        """
        Establish SSL/TLS connection to IMAP server.
        
        Raises:
            IMAPError: If connection fails
        """
        try:
            context = ssl.create_default_context()
            self.connection = imaplib.IMAP4_SSL(
                self.host,
                self.port,
                ssl_context=context,
                timeout=self.timeout
            )
            self.connection.login(self.username, self.password)
            logger.info(f"Connected to {self.host} as {self.username}")
        except imaplib.IMAP4.error as e:
            raise IMAPError(f"IMAP login failed: {e}")
        except Exception as e:
            raise IMAPError(f"Connection failed: {e}")
    
    def disconnect(self) -> None:
        """Close IMAP connection."""
        if self.connection:
            try:
                self.connection.logout()
                logger.info(f"Disconnected from {self.host}")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self.connection = None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
    
    def select_folder(self, folder: str) -> int:
        """
        Select an IMAP folder.
        
        Args:
            folder: Folder name (e.g., 'INBOX', 'Posta inviata')
        
        Returns:
            Number of messages in folder
        
        Raises:
            IMAPError: If folder selection fails
        """
        if not self.connection:
            raise IMAPError("Not connected to IMAP server")
        
        try:
            status, data = self.connection.select(folder, readonly=True)
            if status != 'OK':
                raise IMAPError(f"Failed to select folder '{folder}': {data}")
            
            count = int(data[0])
            logger.debug(f"Selected folder '{folder}' with {count} messages")
            return count
        except imaplib.IMAP4.error as e:
            raise IMAPError(f"Failed to select folder '{folder}': {e}")
    
    def search_by_date(self, target_date: datetime) -> list:
        """
        Search for messages from a specific date.
        
        Args:
            target_date: Date to search for
        
        Returns:
            List of message UIDs
        
        Raises:
            IMAPError: If search fails
        """
        if not self.connection:
            raise IMAPError("Not connected to IMAP server")
        
        # IMAP date format: DD-Mon-YYYY
        date_str = target_date.strftime("%d-%b-%Y")
        
        try:
            # Search for messages on the specific date
            status, data = self.connection.search(None, f'ON {date_str}')
            if status != 'OK':
                raise IMAPError(f"Search failed: {data}")
            
            if data[0]:
                uids = data[0].split()
                logger.debug(f"Found {len(uids)} messages for date {date_str}")
                return uids
            return []
        except imaplib.IMAP4.error as e:
            raise IMAPError(f"Search failed: {e}")
    
    def fetch_message(self, uid: bytes) -> tuple[Message, bytes]:
        """
        Fetch a single message by UID.
        
        Args:
            uid: Message UID
        
        Returns:
            Tuple of (parsed Message object, raw email bytes)
        
        Raises:
            IMAPError: If fetch fails
        """
        if not self.connection:
            raise IMAPError("Not connected to IMAP server")
        
        try:
            status, data = self.connection.fetch(uid, '(RFC822)')
            if status != 'OK':
                raise IMAPError(f"Failed to fetch message {uid}: {data}")
            
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)
            return msg, raw_email
        except imaplib.IMAP4.error as e:
            raise IMAPError(f"Failed to fetch message {uid}: {e}")
    
    def fetch_messages_by_date(
        self,
        folder: str,
        target_date: datetime,
        batch_size: int = 100
    ) -> Generator[tuple[Message, bytes, str], None, None]:
        """
        Fetch all messages from a folder for a specific date.
        
        Args:
            folder: IMAP folder name
            target_date: Date to fetch messages for
            batch_size: Number of messages to fetch per batch
        
        Yields:
            Tuple of (Message object, raw email bytes, UID string)
        
        Raises:
            IMAPError: If fetch fails
        """
        self.select_folder(folder)
        uids = self.search_by_date(target_date)
        
        logger.info(f"Fetching {len(uids)} messages from '{folder}' for {target_date.date()}")
        
        for i in range(0, len(uids), batch_size):
            batch = uids[i:i + batch_size]
            for uid in batch:
                try:
                    msg, raw_email = self.fetch_message(uid)
                    yield msg, raw_email, uid.decode('utf-8')
                except IMAPError as e:
                    logger.error(f"Failed to fetch message {uid}: {e}")
                    continue


def with_retry(
    func,
    max_retries: int = 3,
    initial_delay: int = 5,
    backoff_multiplier: int = 2
):
    """
    Execute function with retry logic and exponential backoff.
    
    Args:
        func: Function to execute
        max_retries: Maximum number of retries
        initial_delay: Initial delay between retries in seconds
        backoff_multiplier: Multiplier for delay on each retry
    
    Returns:
        Function result
    
    Raises:
        Exception: If all retries fail
    """
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                logger.warning(
                    f"Attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {delay} seconds..."
                )
                time.sleep(delay)
                delay *= backoff_multiplier
            else:
                logger.error(f"All {max_retries + 1} attempts failed")
    
    raise last_exception
