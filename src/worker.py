"""
Account Worker module for PEC Archiver.
Processes a single PEC account's mailbox.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional
from email.message import Message

from .imap_client import IMAPClient, IMAPError, with_retry
from .storage import Storage, StorageError
from .indexing import Indexer
from .compression import create_archive, create_digest, CompressionError
from .reporting import create_summary

logger = logging.getLogger(__name__)


class WorkerError(Exception):
    """Worker operation error."""
    pass


class AccountWorker:
    """
    Worker that processes a single PEC account.
    Downloads messages, saves them, creates indexes and archives.
    """
    
    def __init__(
        self,
        account_config: dict,
        base_path: str,
        retry_policy: dict = None,
        imap_settings: dict = None,
        backup_mode: str = 'standard'
    ):
        """
        Initialize account worker.
        
        Args:
            account_config: Account configuration dictionary
            base_path: Base path for archive storage
            retry_policy: Retry policy configuration
            imap_settings: IMAP settings configuration
            backup_mode: Backup mode ('standard' or 's3_sync')
        """
        self.account_config = account_config
        self.base_path = base_path
        self.backup_mode = backup_mode
        self.retry_policy = retry_policy or {
            'max_retries': 3,
            'initial_delay': 5,
            'backoff_multiplier': 2
        }
        self.imap_settings = imap_settings or {
            'timeout': 30,
            'batch_size': 100
        }
        
        self.username = account_config['username']
        self.password = account_config['password']
        self.host = account_config['host']
        self.port = account_config.get('port', 993)
        self.folders = account_config['folders']
        
        self.storage = Storage(base_path)
        self.errors = []
        self.start_time = None
        self.end_time = None
    
    def process(self, target_date: datetime) -> str:
        """
        Process account for a specific date.
        
        Args:
            target_date: Date to archive
        
        Returns:
            Path to summary.json file
        
        Raises:
            WorkerError: If critical error occurs
        """
        self.start_time = datetime.now()
        self.errors = []
        
        account_name = self.username.split('@')[0]
        logger.info(f"Starting processing for {self.username} (date: {target_date.date()})")
        
        # Create directory structure
        try:
            account_path = self.storage.create_directory_structure(
                self.username,
                target_date,
                self.folders
            )
        except StorageError as e:
            self.errors.append({
                'type': 'storage',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            })
            raise WorkerError(f"Failed to create directory structure: {e}")
        
        # Initialize indexer
        indexer = Indexer(account_path)
        
        # Connect to IMAP and fetch messages
        try:
            def connect_and_fetch():
                return self._fetch_messages(target_date, indexer)
            
            with_retry(
                connect_and_fetch,
                max_retries=self.retry_policy['max_retries'],
                initial_delay=self.retry_policy['initial_delay'],
                backoff_multiplier=self.retry_policy['backoff_multiplier']
            )
        except Exception as e:
            self.errors.append({
                'type': 'imap',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            })
            logger.error(f"IMAP error for {self.username}: {e}")
        
        # Generate indexes
        try:
            indexer.generate_all()
        except Exception as e:
            self.errors.append({
                'type': 'indexing',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            })
            logger.error(f"Indexing error for {self.username}: {e}")
        
        # Create archive
        # In standard mode: keep archive locally
        # In s3_sync mode: create archive for S3 upload (will be handled by scheduler)
        archive_path = None
        digest_path = None
        try:
            archive_path = create_archive(
                account_path,
                account_name,
                target_date
            )
            digest_path = create_digest(archive_path)
            
            if self.backup_mode == 's3_sync':
                logger.info(
                    f"Archive created for S3 upload: {archive_path} "
                    "(will be uploaded and removed by scheduler)"
                )
            else:
                logger.info(f"Archive created and kept locally: {archive_path}")
                
        except CompressionError as e:
            self.errors.append({
                'type': 'compression',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            })
            logger.error(f"Compression error for {self.username}: {e}")
        
        # Generate summary
        self.end_time = datetime.now()
        stats = indexer.get_stats()
        
        try:
            summary_path = create_summary(
                account_path=account_path,
                account=self.username,
                date=target_date,
                stats=stats,
                archive_path=archive_path,
                digest_path=digest_path,
                errors=self.errors,
                start_time=self.start_time,
                end_time=self.end_time
            )
        except Exception as e:
            raise WorkerError(f"Failed to create summary: {e}")
        
        duration = (self.end_time - self.start_time).total_seconds()
        logger.info(
            f"Completed processing for {self.username}: "
            f"{stats['total_messages']} messages in {duration:.2f}s"
        )
        
        return summary_path
    
    def _fetch_messages(self, target_date: datetime, indexer: Indexer) -> None:
        """
        Fetch and save messages from all folders.
        
        Args:
            target_date: Date to fetch messages for
            indexer: Indexer to add messages to
        """
        with IMAPClient(
            host=self.host,
            username=self.username,
            password=self.password,
            port=self.port,
            timeout=self.imap_settings['timeout']
        ) as client:
            for folder in self.folders:
                try:
                    self._fetch_folder_messages(
                        client,
                        folder,
                        target_date,
                        indexer
                    )
                except IMAPError as e:
                    self.errors.append({
                        'type': 'imap',
                        'folder': folder,
                        'message': str(e),
                        'timestamp': datetime.now().isoformat()
                    })
                    logger.error(f"Error fetching folder '{folder}': {e}")
    
    def _fetch_folder_messages(
        self,
        client: IMAPClient,
        folder: str,
        target_date: datetime,
        indexer: Indexer
    ) -> None:
        """
        Fetch and save messages from a single folder.
        
        Args:
            client: IMAP client
            folder: Folder name
            target_date: Date to fetch messages for
            indexer: Indexer to add messages to
        """
        for msg, raw_email, uid, flags in client.fetch_messages_by_date(
            folder,
            target_date,
            batch_size=self.imap_settings['batch_size']
        ):
            try:
                filepath = self.storage.save_eml(
                    self.username,
                    target_date,
                    folder,
                    uid,
                    msg,
                    raw_email
                )
                # Check if message is unread (doesn't have \Seen flag)
                # Note: flags are bytes like b'\\Seen', b'\\Answered', etc.
                is_unread = not any(flag.lower() == b'\\seen' for flag in flags)
                indexer.add_message(msg, uid, folder, filepath, is_unread=is_unread)
            except StorageError as e:
                self.errors.append({
                    'type': 'storage',
                    'folder': folder,
                    'uid': uid,
                    'message': str(e),
                    'timestamp': datetime.now().isoformat()
                })
                logger.error(f"Error saving message {uid}: {e}")
