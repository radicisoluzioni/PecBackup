#!/usr/bin/env python3
"""
PEC Archiver - Date Range Backup Script

Auxiliary script to run backup for a specific date or date range.
Useful for emergency scenarios where you need to backup a specific 
week or day.
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Tuple

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_config, ConfigError
from src.scheduler import PECScheduler


def setup_logging(level: str = 'INFO') -> None:
    """
    Configure logging for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def parse_date(date_str: str) -> datetime:
    """
    Parse a date string in YYYY-MM-DD format.
    
    Args:
        date_str: Date string to parse
        
    Returns:
        datetime object
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def validate_date_range(date_from: datetime, date_to: datetime) -> None:
    """
    Validate that a date range is valid.
    
    Args:
        date_from: Start date
        date_to: End date
        
    Raises:
        ValueError: If date range is invalid
    """
    if date_from > date_to:
        raise ValueError(
            f"Start date ({date_from.date()}) must be before or equal to "
            f"end date ({date_to.date()})"
        )
    
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if date_to > today:
        raise ValueError(
            f"End date ({date_to.date()}) cannot be in the future"
        )


def generate_date_range(date_from: datetime, date_to: datetime) -> List[datetime]:
    """
    Generate a list of dates between date_from and date_to (inclusive).
    
    Args:
        date_from: Start date
        date_to: End date
        
    Returns:
        List of datetime objects
    """
    dates = []
    current = date_from
    while current <= date_to:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='PEC Archiver - Backup for a specific date or date range',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backup a specific day
  python -m src.backup_range --date 2024-01-15
  
  # Backup a date range (from/to inclusive)
  python -m src.backup_range --date-from 2024-01-15 --date-to 2024-01-22
  
  # Backup a specific week (last 7 days from a date)
  python -m src.backup_range --date-from 2024-01-15 --date-to 2024-01-21
  
  # Use custom config file
  python -m src.backup_range --date 2024-01-15 --config /path/to/config.yaml
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        default=None,
        help='Path to configuration file (default: uses PEC_ARCHIVE_CONFIG env var)'
    )
    
    parser.add_argument(
        '--date', '-d',
        type=str,
        default=None,
        help='Single date to backup (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--date-from', '-f',
        type=str,
        default=None,
        dest='date_from',
        help='Start date for date range (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--date-to', '-t',
        type=str,
        default=None,
        dest='date_to',
        help='End date for date range (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--log-level', '-l',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> Tuple[datetime, datetime]:
    """
    Validate command line arguments and return the date range.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Tuple of (start_date, end_date)
        
    Raises:
        ValueError: If arguments are invalid
    """
    # Check that at least one date option is provided
    if not args.date and not args.date_from and not args.date_to:
        raise ValueError(
            "You must specify either --date for a single day, "
            "or --date-from and --date-to for a date range"
        )
    
    # Check for conflicting options
    if args.date and (args.date_from or args.date_to):
        raise ValueError(
            "Cannot use --date together with --date-from/--date-to. "
            "Use either --date for a single day, or --date-from and --date-to for a range"
        )
    
    # Handle single date
    if args.date:
        date = parse_date(args.date)
        return date, date
    
    # Handle date range
    if args.date_from and args.date_to:
        date_from = parse_date(args.date_from)
        date_to = parse_date(args.date_to)
        validate_date_range(date_from, date_to)
        return date_from, date_to
    
    # Only one of date_from/date_to provided
    if args.date_from and not args.date_to:
        raise ValueError("--date-from requires --date-to")
    if args.date_to and not args.date_from:
        raise ValueError("--date-to requires --date-from")
    
    # Should not reach here
    raise ValueError("Invalid argument combination")


def main() -> int:
    """
    Main entry point.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    args = parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # Validate arguments
    try:
        date_from, date_to = validate_args(args)
    except ValueError as e:
        logger.error(str(e))
        return 1
    
    # Load configuration
    try:
        config = load_config(args.config)
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    
    # Generate date list
    dates = generate_date_range(date_from, date_to)
    
    if len(dates) == 1:
        logger.info(f"PEC Archiver - Backing up date: {dates[0].date()}")
    else:
        logger.info(
            f"PEC Archiver - Backing up date range: "
            f"{date_from.date()} to {date_to.date()} ({len(dates)} days)"
        )
    
    # Create scheduler
    scheduler = PECScheduler(config=config)
    
    # Process each date
    total_results = {
        'dates_processed': 0,
        'dates_successful': 0,
        'dates_with_errors': 0,
        'total_accounts_processed': 0,
        'total_accounts_successful': 0,
        'total_messages': 0,
        'total_errors': 0
    }
    
    for date in dates:
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing date: {date.date()}")
        logger.info('='*50)
        
        try:
            report = scheduler.run_once(date)
            
            total_results['dates_processed'] += 1
            total_results['total_accounts_processed'] += report['accounts_processed']
            total_results['total_accounts_successful'] += report['accounts_successful']
            total_results['total_messages'] += report['total_messages']
            total_results['total_errors'] += report['total_errors']
            
            if report['accounts_with_errors'] == 0:
                total_results['dates_successful'] += 1
            else:
                total_results['dates_with_errors'] += 1
            
            logger.info(
                f"Date {date.date()} completed: "
                f"{report['accounts_successful']}/{report['accounts_processed']} accounts, "
                f"{report['total_messages']} messages"
            )
        except Exception as e:
            logger.error(f"Failed to process date {date.date()}: {e}")
            total_results['dates_processed'] += 1
            total_results['dates_with_errors'] += 1
    
    # Print final summary
    print("\n" + "="*60)
    print("BACKUP RANGE JOB COMPLETED")
    print("="*60)
    print(f"Date range: {date_from.date()} to {date_to.date()}")
    print(f"Days processed: {total_results['dates_processed']}")
    print(f"Days successful: {total_results['dates_successful']}")
    print(f"Days with errors: {total_results['dates_with_errors']}")
    print("-"*60)
    print(f"Total accounts processed: {total_results['total_accounts_processed']}")
    print(f"Total accounts successful: {total_results['total_accounts_successful']}")
    print(f"Total messages: {total_results['total_messages']}")
    print(f"Total errors: {total_results['total_errors']}")
    print("="*60 + "\n")
    
    return 0 if total_results['dates_with_errors'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
