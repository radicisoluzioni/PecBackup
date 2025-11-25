"""
Reporting module for PEC Archiver.
Generates summary.json with archive statistics and status.
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class ReportingError(Exception):
    """Reporting operation error."""
    pass


def create_summary(
    account_path: str,
    account: str,
    date: datetime,
    stats: dict,
    archive_path: Optional[str] = None,
    digest_path: Optional[str] = None,
    errors: list[dict] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> str:
    """
    Create summary.json file with archive statistics.
    
    Args:
        account_path: Path to account's date directory
        account: Account username
        date: Archive date
        stats: Statistics from indexer
        archive_path: Path to created archive
        digest_path: Path to digest file
        errors: List of error dictionaries
        start_time: Processing start time
        end_time: Processing end time
    
    Returns:
        Path to summary.json file
    
    Raises:
        ReportingError: If summary creation fails
    """
    if errors is None:
        errors = []
    
    summary = {
        'account': account,
        'date': date.strftime('%Y-%m-%d'),
        'generated_at': datetime.now().isoformat(),
        'status': 'success' if not errors else 'completed_with_errors',
        'statistics': {
            'total_messages': stats.get('total_messages', 0),
            'total_size_bytes': stats.get('total_size', 0),
            'folders': stats.get('folders', {})
        },
        'files': {
            'index_csv': 'index.csv',
            'index_json': 'index.json'
        },
        'processing': {}
    }
    
    if archive_path:
        summary['files']['archive'] = os.path.basename(archive_path)
        summary['archive'] = {
            'filename': os.path.basename(archive_path),
            'size_bytes': os.path.getsize(archive_path) if os.path.exists(archive_path) else 0
        }
    
    if digest_path:
        summary['files']['digest'] = os.path.basename(digest_path)
        # Read digest value
        try:
            with open(digest_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                summary['archive']['sha256'] = content.split()[0]
        except Exception:
            pass
    
    if start_time:
        summary['processing']['start_time'] = start_time.isoformat()
    if end_time:
        summary['processing']['end_time'] = end_time.isoformat()
    if start_time and end_time:
        duration = (end_time - start_time).total_seconds()
        summary['processing']['duration_seconds'] = duration
    
    if errors:
        summary['errors'] = errors
        summary['error_count'] = len(errors)
    
    summary_path = os.path.join(account_path, 'summary.json')
    
    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Created summary: {summary_path}")
        return summary_path
    except Exception as e:
        raise ReportingError(f"Failed to create summary: {e}")


def format_summary_for_log(summary_path: str) -> str:
    """
    Format summary for logging output.
    
    Args:
        summary_path: Path to summary.json file
    
    Returns:
        Formatted summary string
    """
    try:
        with open(summary_path, 'r', encoding='utf-8') as f:
            summary = json.load(f)
        
        lines = [
            f"Account: {summary.get('account', 'N/A')}",
            f"Date: {summary.get('date', 'N/A')}",
            f"Status: {summary.get('status', 'N/A')}",
            f"Messages: {summary.get('statistics', {}).get('total_messages', 0)}",
        ]
        
        folders = summary.get('statistics', {}).get('folders', {})
        for folder, count in folders.items():
            lines.append(f"  - {folder}: {count}")
        
        if summary.get('archive'):
            archive = summary['archive']
            size_mb = archive.get('size_bytes', 0) / (1024 * 1024)
            lines.append(f"Archive: {archive.get('filename', 'N/A')} ({size_mb:.2f} MB)")
        
        if summary.get('errors'):
            lines.append(f"Errors: {len(summary['errors'])}")
        
        return '\n'.join(lines)
    except Exception as e:
        return f"Failed to format summary: {e}"


def aggregate_summaries(summary_paths: list[str]) -> dict:
    """
    Aggregate multiple summaries into a single report.
    
    Args:
        summary_paths: List of paths to summary.json files
    
    Returns:
        Aggregated report dictionary
    """
    report = {
        'generated_at': datetime.now().isoformat(),
        'accounts_processed': 0,
        'accounts_successful': 0,
        'accounts_with_errors': 0,
        'total_messages': 0,
        'total_size_bytes': 0,
        'total_errors': 0,
        'accounts': []
    }
    
    for summary_path in summary_paths:
        try:
            with open(summary_path, 'r', encoding='utf-8') as f:
                summary = json.load(f)
            
            report['accounts_processed'] += 1
            
            if summary.get('status') == 'success':
                report['accounts_successful'] += 1
            else:
                report['accounts_with_errors'] += 1
            
            stats = summary.get('statistics', {})
            report['total_messages'] += stats.get('total_messages', 0)
            report['total_size_bytes'] += stats.get('total_size_bytes', 0)
            report['total_errors'] += summary.get('error_count', 0)
            
            report['accounts'].append({
                'account': summary.get('account'),
                'status': summary.get('status'),
                'messages': stats.get('total_messages', 0)
            })
        except Exception as e:
            logger.error(f"Failed to read summary {summary_path}: {e}")
    
    return report
