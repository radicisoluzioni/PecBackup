"""
Tests for REST API module.
"""

import os
import json
import tempfile
import pytest
from datetime import datetime
from email.message import EmailMessage

from fastapi.testclient import TestClient

from src.api import app, set_base_path, get_base_path
from src.indexing import Indexer
from src.storage import Storage


@pytest.fixture
def temp_archive():
    """Create a temporary archive with test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test account structure
        account_name = "test"
        date_str = "2024-01-15"
        year = "2024"
        
        storage = Storage(tmpdir)
        target_date = datetime(2024, 1, 15)
        
        # Create directory structure
        folders = ["INBOX", "Posta inviata"]
        storage.create_directory_structure(f"{account_name}@pec.it", target_date, folders)
        
        # Create test messages
        messages = []
        for i in range(3):
            msg = EmailMessage()
            msg["Subject"] = f"Test Subject {i+1}"
            msg["From"] = f"sender{i+1}@example.com"
            msg["To"] = f"recipient{i+1}@example.com"
            msg["Date"] = f"Mon, 15 Jan 2024 10:{i}0:00 +0100"
            msg["Message-ID"] = f"<{i+1}@example.com>"
            msg.set_content(f"Test body {i+1}")
            
            folder = "INBOX" if i < 2 else "Posta inviata"
            filepath = storage.save_eml(
                f"{account_name}@pec.it",
                target_date,
                folder,
                str(i + 1),
                msg,
                msg.as_bytes()
            )
            messages.append((msg, str(i + 1), folder, filepath))
        
        # Create index
        account_path = storage.get_account_path(f"{account_name}@pec.it", target_date)
        indexer = Indexer(account_path)
        for msg, uid, folder, filepath in messages:
            indexer.add_message(msg, uid, folder, filepath)
        indexer.generate_json()
        
        yield tmpdir, account_name, date_str


@pytest.fixture
def client(temp_archive):
    """Create test client with temporary archive."""
    tmpdir, _, _ = temp_archive
    set_base_path(tmpdir)
    return TestClient(app)


class TestHealthEndpoint:
    """Tests for health check endpoint."""
    
    def test_health_check(self, client):
        """Test health check returns healthy status."""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data


class TestAccountsEndpoint:
    """Tests for accounts endpoint."""
    
    def test_list_accounts(self, client, temp_archive):
        """Test listing accounts."""
        _, account_name, _ = temp_archive
        response = client.get("/api/v1/accounts")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert len(data["accounts"]) == 1
        assert data["accounts"][0]["name"] == account_name
        assert "2024" in data["accounts"][0]["years"]
    
    def test_list_dates(self, client, temp_archive):
        """Test listing dates for an account."""
        _, account_name, date_str = temp_archive
        response = client.get(f"/api/v1/accounts/{account_name}/dates?year=2024")
        assert response.status_code == 200
        data = response.json()
        assert data["account"] == account_name
        assert data["year"] == "2024"
        assert data["total"] == 1
        assert data["dates"][0]["date"] == date_str
        assert data["dates"][0]["message_count"] == 3
    
    def test_list_dates_account_not_found(self, client):
        """Test listing dates for non-existent account."""
        response = client.get("/api/v1/accounts/nonexistent/dates?year=2024")
        assert response.status_code == 404


class TestEmailsEndpoint:
    """Tests for emails endpoint."""
    
    def test_list_emails(self, client, temp_archive):
        """Test listing emails for a specific date."""
        _, account_name, date_str = temp_archive
        response = client.get(f"/api/v1/accounts/{account_name}/emails/{date_str}")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert len(data["emails"]) == 3
    
    def test_list_emails_with_folder_filter(self, client, temp_archive):
        """Test listing emails filtered by folder."""
        _, account_name, date_str = temp_archive
        response = client.get(f"/api/v1/accounts/{account_name}/emails/{date_str}?folder=INBOX")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert all(e["folder"] == "INBOX" for e in data["emails"])
    
    def test_list_emails_invalid_date(self, client, temp_archive):
        """Test listing emails with invalid date format."""
        _, account_name, _ = temp_archive
        response = client.get(f"/api/v1/accounts/{account_name}/emails/invalid-date")
        assert response.status_code == 400
    
    def test_list_emails_not_found(self, client, temp_archive):
        """Test listing emails for non-existent date."""
        _, account_name, _ = temp_archive
        response = client.get(f"/api/v1/accounts/{account_name}/emails/2023-01-01")
        assert response.status_code == 404


class TestSearchEndpoint:
    """Tests for search endpoint."""
    
    def test_search_by_subject(self, client, temp_archive):
        """Test searching by subject."""
        response = client.get("/api/v1/search?subject=Test Subject 1")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert "Test Subject 1" in data["results"][0]["email"]["subject"]
    
    def test_search_by_sender(self, client, temp_archive):
        """Test searching by sender."""
        response = client.get("/api/v1/search?from=sender1@example.com")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
    
    def test_search_by_recipient(self, client, temp_archive):
        """Test searching by recipient."""
        response = client.get("/api/v1/search?to=recipient2@example.com")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
    
    def test_search_by_account(self, client, temp_archive):
        """Test searching by account."""
        _, account_name, _ = temp_archive
        response = client.get(f"/api/v1/search?account={account_name}")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
    
    def test_search_by_date_range(self, client, temp_archive):
        """Test searching by date range."""
        response = client.get("/api/v1/search?date_from=2024-01-14&date_to=2024-01-16")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
    
    def test_search_no_params(self, client):
        """Test search without parameters returns error."""
        response = client.get("/api/v1/search")
        assert response.status_code == 400
    
    def test_search_no_results(self, client, temp_archive):
        """Test search with no matching results."""
        response = client.get("/api/v1/search?subject=NonExistentSubject")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert len(data["results"]) == 0
    
    def test_search_pagination(self, client, temp_archive):
        """Test search pagination."""
        _, account_name, _ = temp_archive
        response = client.get(f"/api/v1/search?account={account_name}&limit=1&offset=0")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert len(data["results"]) == 1


class TestDownloadEndpoint:
    """Tests for download endpoint."""
    
    def test_download_email(self, client, temp_archive):
        """Test downloading an email file."""
        tmpdir, account_name, date_str = temp_archive
        
        # First, get the list of emails to find a valid filename
        response = client.get(f"/api/v1/accounts/{account_name}/emails/{date_str}")
        assert response.status_code == 200
        emails = response.json()["emails"]
        
        # Download the first email
        email = emails[0]
        folder = email["folder"]
        filename = email["filename"]
        
        response = client.get(
            f"/api/v1/accounts/{account_name}/emails/{date_str}/{folder}/{filename}"
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "message/rfc822"
    
    def test_download_email_not_found(self, client, temp_archive):
        """Test downloading non-existent email."""
        _, account_name, date_str = temp_archive
        response = client.get(
            f"/api/v1/accounts/{account_name}/emails/{date_str}/INBOX/nonexistent.eml"
        )
        assert response.status_code == 404
    
    def test_download_invalid_file_type(self, client, temp_archive):
        """Test downloading non-.eml file."""
        _, account_name, date_str = temp_archive
        response = client.get(
            f"/api/v1/accounts/{account_name}/emails/{date_str}/INBOX/test.txt"
        )
        # Should return 404 (file not found) or 400 (invalid file type)
        assert response.status_code in [400, 404]


class TestPathTraversal:
    """Tests for path traversal prevention."""
    
    def test_path_traversal_in_account(self, client, temp_archive):
        """Test path traversal prevention in account parameter."""
        _, _, date_str = temp_archive
        response = client.get(f"/api/v1/accounts/../etc/emails/{date_str}")
        # Should fail due to path validation
        assert response.status_code in [400, 404]
    
    def test_path_traversal_in_folder(self, client, temp_archive):
        """Test path traversal prevention in folder parameter."""
        _, account_name, date_str = temp_archive
        response = client.get(
            f"/api/v1/accounts/{account_name}/emails/{date_str}/../../../etc/test.eml"
        )
        assert response.status_code in [400, 404]
    
    def test_path_traversal_in_filename(self, client, temp_archive):
        """Test path traversal prevention in filename parameter."""
        _, account_name, date_str = temp_archive
        response = client.get(
            f"/api/v1/accounts/{account_name}/emails/{date_str}/INBOX/../../test.eml"
        )
        assert response.status_code in [400, 404]


class TestEmptyArchive:
    """Tests with empty archive."""
    
    def test_list_accounts_empty(self):
        """Test listing accounts with empty archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            set_base_path(tmpdir)
            client = TestClient(app)
            response = client.get("/api/v1/accounts")
            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 0
            assert len(data["accounts"]) == 0
    
    def test_search_empty(self):
        """Test searching in empty archive."""
        with tempfile.TemporaryDirectory() as tmpdir:
            set_base_path(tmpdir)
            client = TestClient(app)
            response = client.get("/api/v1/search?subject=test")
            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 0
