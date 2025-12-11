"""
Scheduler module for PEC Archiver.
Main scheduler that runs daily at configured time.
"""

from __future__ import annotations

import schedule
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Tuple

from .config import load_config
from .worker import AccountWorker, WorkerError
from .reporting import aggregate_summaries
from .notifications import send_notification, NotificationError

logger = logging.getLogger(__name__)


class SchedulerError(Exception):
    """Scheduler operation error."""
    pass


class PECScheduler:
    """
    Main scheduler for PEC archiving.
    Runs daily at configured time to archive previous day's messages.
    """
    
    def __init__(self, config: dict = None, config_path: str = None):
        """
        Initialize scheduler.
        
        Args:
            config: Configuration dictionary (optional)
            config_path: Path to configuration file (optional)
        """
        if config:
            self.config = config
        else:
            self.config = load_config(config_path)
        
        self.base_path = self.config['base_path']
        self.concurrency = self.config.get('concurrency', 4)
        self.retry_policy = self.config.get('retry_policy', {})
        self.imap_settings = self.config.get('imap', {})
        self.accounts = self.config['accounts']
        self.run_time = self.config.get('scheduler', {}).get('run_time', '01:00')
        self.notifications_config = self.config.get('notifications', {})
        self.backup_mode = self.config.get('backup_mode', 'standard')
        self.s3_config = self.config.get('s3', {})
        
        # Initialize S3 storage if in s3_sync mode
        self.s3_storage = None
        if self.backup_mode == 's3_sync':
            from .s3_storage import S3Storage, S3StorageError
            try:
                self.s3_storage = S3Storage(self.s3_config)
                # Verify S3 bucket access
                if not self.s3_storage.verify_bucket_access():
                    logger.warning("S3 bucket access verification failed")
            except S3StorageError as e:
                logger.error(f"Failed to initialize S3 storage: {e}")
                raise
    
    def run_archive_job(self, target_date: datetime = None) -> dict:
        """
        Run the archive job for all accounts.
        
        Args:
            target_date: Date to archive (default: yesterday)
        
        Returns:
            Aggregated report dictionary
        """
        if target_date is None:
            target_date = datetime.now() - timedelta(days=1)
            # Set to beginning of day
            target_date = target_date.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        
        logger.info(f"Starting archive job for date: {target_date.date()}")
        logger.info(f"Backup mode: {self.backup_mode}")
        logger.info(f"Processing {len(self.accounts)} accounts with {self.concurrency} workers")
        
        summary_paths = []
        account_results = []  # Store results for S3 upload
        
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = {}
            
            for account in self.accounts:
                worker = AccountWorker(
                    account_config=account,
                    base_path=self.base_path,
                    retry_policy=self.retry_policy,
                    imap_settings=self.imap_settings,
                    backup_mode=self.backup_mode
                )
                future = executor.submit(worker.process, target_date)
                futures[future] = account['username']
            
            for future in as_completed(futures):
                username = futures[future]
                try:
                    summary_path = future.result()
                    summary_paths.append(summary_path)
                    account_results.append((username, summary_path))
                    logger.info(f"Completed: {username}")
                except WorkerError as e:
                    logger.error(f"Worker error for {username}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error for {username}: {e}")
        
        # Handle S3 uploads if in s3_sync mode
        if self.backup_mode == 's3_sync' and self.s3_storage:
            self._handle_s3_uploads(account_results, target_date)
        
        # Aggregate summaries
        report = aggregate_summaries(summary_paths)
        
        logger.info(
            f"Archive job completed: "
            f"{report['accounts_successful']}/{report['accounts_processed']} successful, "
            f"{report['total_messages']} total messages"
        )
        
        # Send notification if configured
        self._send_notification(report, target_date)
        
        return report
    
    def _handle_s3_uploads(
        self,
        account_results: List[Tuple[str, str]],
        target_date: datetime
    ) -> None:
        """
        Handle S3 uploads for all accounts in s3_sync mode.
        
        Args:
            account_results: List of (username, summary_path) tuples
            target_date: Date that was archived
        """
        import os
        import json
        from .storage import Storage
        from .s3_storage import S3StorageError
        
        logger.info("Starting S3 uploads for daily archives...")
        
        storage = Storage(self.base_path)
        
        for username, summary_path in account_results:
            try:
                # Read summary to get archive and digest paths
                with open(summary_path, 'r', encoding='utf-8') as f:
                    summary = json.load(f)
                
                archive_path = summary.get('archive_path')
                digest_path = summary.get('digest_path')
                
                if not archive_path or not os.path.exists(archive_path):
                    logger.warning(f"No archive found for {username}, skipping S3 upload")
                    continue
                
                # Upload to S3
                logger.info(f"Uploading archive for {username} to S3...")
                result = self.s3_storage.upload_archive(
                    archive_path=archive_path,
                    account=username,
                    date=target_date,
                    digest_path=digest_path
                )
                
                logger.info(
                    f"Successfully uploaded to S3: s3://{result['bucket']}/{result['s3_key']}"
                )
                
                # Delete local archive and digest after successful upload
                # Also clean up the temporary dated directory
                try:
                    archive_dir = os.path.dirname(archive_path)
                    
                    if os.path.exists(archive_path):
                        os.remove(archive_path)
                        logger.info(f"Deleted local archive: {archive_path}")
                    
                    if digest_path and os.path.exists(digest_path):
                        os.remove(digest_path)
                        logger.info(f"Deleted local digest: {digest_path}")
                    
                    # Remove the temporary dated directory if it's empty
                    # (it should only contain the archive and digest we just deleted)
                    if os.path.exists(archive_dir) and not os.listdir(archive_dir):
                        os.rmdir(archive_dir)
                        logger.info(f"Deleted empty dated directory: {archive_dir}")
                        
                        # Also try to remove the year directory if empty
                        year_dir = os.path.dirname(archive_dir)
                        if os.path.exists(year_dir) and not os.listdir(year_dir):
                            os.rmdir(year_dir)
                            logger.info(f"Deleted empty year directory: {year_dir}")
                    
                except OSError as e:
                    logger.warning(f"Failed to delete local files: {e}")
                
            except S3StorageError as e:
                logger.error(f"S3 upload failed for {username}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during S3 upload for {username}: {e}")
        
        logger.info("S3 uploads completed")
    
    def _send_notification(self, report: dict, target_date: datetime) -> None:
        """
        Send notification with backup report.
        
        Args:
            report: Aggregated report dictionary
            target_date: Date that was archived
        """
        try:
            sent = send_notification(
                config=self.notifications_config,
                report=report,
                target_date=target_date
            )
            if sent:
                logger.info("Notification sent successfully")
        except NotificationError as e:
            logger.error(f"Failed to send notification: {e}")
    
    def schedule_daily(self) -> None:
        """Schedule the archive job to run daily at configured time."""
        schedule.every().day.at(self.run_time).do(self.run_archive_job)
        logger.info(f"Scheduled daily archive job at {self.run_time}")
    
    def start(self) -> None:
        """Start the scheduler and run indefinitely."""
        self.schedule_daily()
        
        logger.info("PEC Archiver scheduler started")
        logger.info(f"Waiting for scheduled time: {self.run_time}")
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    def run_once(self, target_date: datetime = None) -> dict:
        """
        Run the archive job once immediately.
        
        Args:
            target_date: Date to archive (default: yesterday)
        
        Returns:
            Aggregated report dictionary
        """
        return self.run_archive_job(target_date)
