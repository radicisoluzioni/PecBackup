"""
REST API module for PEC Archiver.
Provides endpoints to search and download archived emails.
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, date
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# API Application
app = FastAPI(
    title="PEC Archiver API",
    description="REST API per ricerche e download delle email PEC archiviate",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)


# Pydantic Models
class EmailInfo(BaseModel):
    """Email information model."""
    uid: str
    folder: str
    filename: str
    filepath: str
    subject: str
    sender: str = Field(alias="from")
    to: str
    cc: Optional[str] = ""
    date: str
    message_id: str
    size: int

    model_config = {"populate_by_name": True}


class EmailListResponse(BaseModel):
    """Response model for email list."""
    total: int
    emails: list[EmailInfo]


class AccountInfo(BaseModel):
    """Account information model."""
    name: str
    years: list[str]


class AccountListResponse(BaseModel):
    """Response model for account list."""
    total: int
    accounts: list[AccountInfo]


class DateInfo(BaseModel):
    """Date information model."""
    date: str
    message_count: int


class DateListResponse(BaseModel):
    """Response model for date list."""
    account: str
    year: str
    total: int
    dates: list[DateInfo]


class SearchResult(BaseModel):
    """Search result model."""
    account: str
    date: str
    email: EmailInfo


class SearchResponse(BaseModel):
    """Response model for search results."""
    total: int
    results: list[SearchResult]


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    timestamp: str


# Global variable for archive base path
_base_path: Optional[str] = None


def set_base_path(path: str) -> None:
    """Set the base path for the archive."""
    global _base_path
    _base_path = path


def get_base_path() -> str:
    """Get the base path for the archive."""
    if _base_path:
        return _base_path
    return os.environ.get("PEC_ARCHIVE_BASE_PATH", "/data/pec-archive")


def load_index_json(account_path: str, date_dir: str) -> list[dict]:
    """
    Load index.json from a specific account and date directory.
    
    Args:
        account_path: Path to account directory
        date_dir: Date directory name (YYYY-MM-DD)
    
    Returns:
        List of email metadata dictionaries
    """
    # Find the year subdirectory
    year = date_dir[:4]
    index_path = os.path.join(account_path, year, date_dir, "index.json")
    
    if not os.path.exists(index_path):
        return []
    
    try:
        with open(index_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Failed to load index.json from {index_path}: {e}")
        return []


def get_accounts() -> list[dict]:
    """
    Get list of all accounts in the archive.
    
    Returns:
        List of account info dictionaries
    """
    base_path = get_base_path()
    accounts = []
    
    if not os.path.exists(base_path):
        return accounts
    
    for account_name in os.listdir(base_path):
        account_path = os.path.join(base_path, account_name)
        if os.path.isdir(account_path):
            years = []
            for year_dir in os.listdir(account_path):
                year_path = os.path.join(account_path, year_dir)
                if os.path.isdir(year_path) and year_dir.isdigit():
                    years.append(year_dir)
            accounts.append({
                "name": account_name,
                "years": sorted(years, reverse=True)
            })
    
    return accounts


def get_dates_for_account(account: str, year: str) -> list[dict]:
    """
    Get list of archived dates for an account in a specific year.
    
    Args:
        account: Account name
        year: Year string (YYYY)
    
    Returns:
        List of date info dictionaries
    """
    base_path = get_base_path()
    year_path = os.path.join(base_path, account, year)
    dates = []
    
    if not os.path.exists(year_path):
        return dates
    
    for date_dir in os.listdir(year_path):
        date_path = os.path.join(year_path, date_dir)
        if os.path.isdir(date_path):
            # Count messages from index.json
            index_data = load_index_json(os.path.join(base_path, account), date_dir)
            dates.append({
                "date": date_dir,
                "message_count": len(index_data)
            })
    
    return sorted(dates, key=lambda x: x["date"], reverse=True)


def search_emails(
    subject: Optional[str] = None,
    sender: Optional[str] = None,
    recipient: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    account: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> tuple[list[dict], int]:
    """
    Search emails across all accounts and dates.
    
    Args:
        subject: Subject filter (case-insensitive partial match)
        sender: Sender filter (case-insensitive partial match)
        recipient: Recipient filter (case-insensitive partial match)
        date_from: Start date filter
        date_to: End date filter
        account: Account filter
        limit: Maximum results to return
        offset: Offset for pagination
    
    Returns:
        Tuple of (list of matching emails, total count)
    """
    base_path = get_base_path()
    results = []
    
    if not os.path.exists(base_path):
        return [], 0
    
    accounts_to_search = []
    if account:
        account_path = os.path.join(base_path, account)
        if os.path.exists(account_path):
            accounts_to_search = [account]
    else:
        accounts_to_search = [
            d for d in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, d))
        ]
    
    for acc in accounts_to_search:
        account_path = os.path.join(base_path, acc)
        
        # Iterate through years
        for year_dir in os.listdir(account_path):
            year_path = os.path.join(account_path, year_dir)
            if not os.path.isdir(year_path) or not year_dir.isdigit():
                continue
            
            # Iterate through dates
            for date_dir in os.listdir(year_path):
                date_path = os.path.join(year_path, date_dir)
                if not os.path.isdir(date_path):
                    continue
                
                # Check date filter
                try:
                    email_date = datetime.strptime(date_dir, "%Y-%m-%d").date()
                    if date_from and email_date < date_from:
                        continue
                    if date_to and email_date > date_to:
                        continue
                except ValueError:
                    continue
                
                # Load index and search
                index_data = load_index_json(account_path, date_dir)
                for email in index_data:
                    # Apply filters
                    if subject:
                        email_subject = email.get("subject", "").lower()
                        if subject.lower() not in email_subject:
                            continue
                    
                    if sender:
                        email_sender = email.get("from", "").lower()
                        if sender.lower() not in email_sender:
                            continue
                    
                    if recipient:
                        email_to = email.get("to", "").lower()
                        email_cc = email.get("cc", "").lower()
                        if recipient.lower() not in email_to and recipient.lower() not in email_cc:
                            continue
                    
                    # Add to results
                    results.append({
                        "account": acc,
                        "date": date_dir,
                        "email": email
                    })
    
    # Sort by date (newest first)
    results.sort(key=lambda x: x["date"], reverse=True)
    
    total = len(results)
    return results[offset:offset + limit], total


# API Endpoints
@app.get("/api/v1/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Check API health status."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat()
    )


@app.get("/api/v1/accounts", response_model=AccountListResponse, tags=["Accounts"])
async def list_accounts():
    """
    List all archived PEC accounts.
    
    Returns the list of account names and their archived years.
    """
    accounts = get_accounts()
    return AccountListResponse(
        total=len(accounts),
        accounts=[AccountInfo(**acc) for acc in accounts]
    )


@app.get("/api/v1/accounts/{account}/dates", response_model=DateListResponse, tags=["Accounts"])
async def list_dates(
    account: str,
    year: str = Query(..., description="Year (YYYY format)", pattern=r"^\d{4}$")
):
    """
    List archived dates for a specific account and year.
    
    Args:
        account: Account name
        year: Year in YYYY format
    """
    base_path = get_base_path()
    account_path = os.path.join(base_path, account)
    
    if not os.path.exists(account_path):
        raise HTTPException(status_code=404, detail=f"Account '{account}' not found")
    
    dates = get_dates_for_account(account, year)
    
    return DateListResponse(
        account=account,
        year=year,
        total=len(dates),
        dates=[DateInfo(**d) for d in dates]
    )


@app.get("/api/v1/accounts/{account}/emails/{date_str}", response_model=EmailListResponse, tags=["Emails"])
async def list_emails(
    account: str,
    date_str: str,
    folder: Optional[str] = Query(None, description="Filter by folder (e.g., INBOX)")
):
    """
    List all emails for a specific account and date.
    
    Args:
        account: Account name
        date_str: Date in YYYY-MM-DD format
        folder: Optional folder filter
    """
    # Validate date format
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    base_path = get_base_path()
    account_path = os.path.join(base_path, account)
    
    if not os.path.exists(account_path):
        raise HTTPException(status_code=404, detail=f"Account '{account}' not found")
    
    index_data = load_index_json(account_path, date_str)
    
    if not index_data:
        raise HTTPException(status_code=404, detail=f"No emails found for date {date_str}")
    
    # Filter by folder if specified
    if folder:
        index_data = [e for e in index_data if e.get("folder", "").lower() == folder.lower()]
    
    return EmailListResponse(
        total=len(index_data),
        emails=[EmailInfo(**email) for email in index_data]
    )


@app.get("/api/v1/search", response_model=SearchResponse, tags=["Search"])
async def search(
    subject: Optional[str] = Query(None, description="Search in subject (case-insensitive)"),
    sender: Optional[str] = Query(None, description="Search in sender (case-insensitive)", alias="from"),
    recipient: Optional[str] = Query(None, description="Search in recipient (case-insensitive)", alias="to"),
    date_from: Optional[date] = Query(None, description="Filter from date (YYYY-MM-DD)"),
    date_to: Optional[date] = Query(None, description="Filter to date (YYYY-MM-DD)"),
    account: Optional[str] = Query(None, description="Filter by account"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Search emails across all accounts.
    
    Supports filtering by subject, sender, recipient, date range, and account.
    Results are sorted by date (newest first).
    """
    if not any([subject, sender, recipient, date_from, date_to, account]):
        raise HTTPException(
            status_code=400,
            detail="At least one search parameter is required"
        )
    
    results, total = search_emails(
        subject=subject,
        sender=sender,
        recipient=recipient,
        date_from=date_from,
        date_to=date_to,
        account=account,
        limit=limit,
        offset=offset
    )
    
    return SearchResponse(
        total=total,
        results=[
            SearchResult(
                account=r["account"],
                date=r["date"],
                email=EmailInfo(**r["email"])
            )
            for r in results
        ]
    )


@app.get(
    "/api/v1/accounts/{account}/emails/{date_str}/{folder}/{filename}",
    tags=["Downloads"],
    response_class=FileResponse
)
async def download_email(
    account: str,
    date_str: str,
    folder: str,
    filename: str
):
    """
    Download a specific email file (.eml).
    
    Args:
        account: Account name
        date_str: Date in YYYY-MM-DD format
        folder: Folder name (e.g., INBOX, Posta_inviata)
        filename: Email filename (e.g., 123_subject.eml)
    
    Returns:
        The .eml file as download
    """
    # Validate date format and extract year
    try:
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        year = str(parsed_date.year)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    base_path = get_base_path()
    
    # Sanitize inputs to prevent path traversal
    safe_account = os.path.basename(account)
    safe_folder = os.path.basename(folder)
    safe_filename = os.path.basename(filename)
    safe_date = os.path.basename(date_str)
    
    file_path = os.path.join(
        base_path,
        safe_account,
        year,
        safe_date,
        safe_folder,
        safe_filename
    )
    
    # Ensure the path is within the base path
    abs_file_path = os.path.abspath(file_path)
    abs_base_path = os.path.abspath(base_path)
    
    if not abs_file_path.startswith(abs_base_path):
        raise HTTPException(status_code=400, detail="Invalid path")
    
    if not os.path.exists(abs_file_path):
        raise HTTPException(status_code=404, detail="Email file not found")
    
    if not abs_file_path.endswith(".eml"):
        raise HTTPException(status_code=400, detail="Only .eml files can be downloaded")
    
    return FileResponse(
        path=abs_file_path,
        filename=safe_filename,
        media_type="message/rfc822"
    )


@app.get(
    "/api/v1/accounts/{account}/archive/{date_str}",
    tags=["Downloads"],
    response_class=FileResponse
)
async def download_archive(account: str, date_str: str):
    """
    Download the compressed archive (.tar.gz) for a specific date.
    
    Args:
        account: Account name
        date_str: Date in YYYY-MM-DD format
    
    Returns:
        The .tar.gz archive file
    """
    # Validate date format and extract year
    try:
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        year = str(parsed_date.year)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    base_path = get_base_path()
    
    # Sanitize inputs
    safe_account = os.path.basename(account)
    safe_date = os.path.basename(date_str)
    
    date_path = os.path.join(base_path, safe_account, year, safe_date)
    
    # Ensure the path is within the base path
    abs_date_path = os.path.abspath(date_path)
    abs_base_path = os.path.abspath(base_path)
    
    if not abs_date_path.startswith(abs_base_path):
        raise HTTPException(status_code=400, detail="Invalid path")
    
    if not os.path.exists(abs_date_path):
        raise HTTPException(status_code=404, detail="Archive date not found")
    
    # Find the archive file
    for file in os.listdir(abs_date_path):
        if file.endswith(".tar.gz"):
            archive_path = os.path.join(abs_date_path, file)
            return FileResponse(
                path=archive_path,
                filename=file,
                media_type="application/gzip"
            )
    
    raise HTTPException(status_code=404, detail="Archive file not found")


def create_app(base_path: Optional[str] = None) -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Args:
        base_path: Optional base path for the archive
    
    Returns:
        Configured FastAPI application
    """
    if base_path:
        set_base_path(base_path)
    return app
