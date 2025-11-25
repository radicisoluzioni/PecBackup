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
from typing import Optional

from .config import load_config
from .worker import AccountWorker, WorkerError
from .reporting import aggregate_summaries

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
        logger.info(f"Processing {len(self.accounts)} accounts with {self.concurrency} workers")
        
        summary_paths = []
        
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = {}
            
            for account in self.accounts:
                worker = AccountWorker(
                    account_config=account,
                    base_path=self.base_path,
                    retry_policy=self.retry_policy,
                    imap_settings=self.imap_settings
                )
                future = executor.submit(worker.process, target_date)
                futures[future] = account['username']
            
            for future in as_completed(futures):
                username = futures[future]
                try:
                    summary_path = future.result()
                    summary_paths.append(summary_path)
                    logger.info(f"Completed: {username}")
                except WorkerError as e:
                    logger.error(f"Worker error for {username}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error for {username}: {e}")
        
        # Aggregate summaries
        report = aggregate_summaries(summary_paths)
        
        logger.info(
            f"Archive job completed: "
            f"{report['accounts_successful']}/{report['accounts_processed']} successful, "
            f"{report['total_messages']} total messages"
        )
        
        return report
    
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
