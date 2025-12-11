"""
Tests for storage module.
"""

import os
import tempfile
import pytest
from datetime import datetime
from email.message import EmailMessage

from src.storage import Storage, sanitize_filename, sanitize_folder_name


class TestSanitizeFilename:
    """Tests for filename sanitization."""
    
    def test_sanitize_simple_filename(self):
        """Test that simple filename passes through."""
        assert sanitize_filename('hello.txt') == 'hello.txt'
    
    def test_sanitize_invalid_chars(self):
        """Test that invalid characters are replaced."""
        result = sanitize_filename('file<>:"/\\|?*name.txt')
        assert '<' not in result
        assert '>' not in result
        assert ':' not in result
        assert '"' not in result
        assert '?' not in result
        assert '*' not in result
    
    def test_sanitize_long_filename(self):
        """Test that long filenames are truncated."""
        long_name = 'a' * 300
        result = sanitize_filename(long_name)
        assert len(result) <= 200


class TestSanitizeFolderName:
    """Tests for folder name sanitization."""
    
    def test_sanitize_spaces(self):
        """Test that spaces are replaced with underscores."""
        result = sanitize_folder_name('Posta inviata')
        assert result == 'Posta_inviata'
    
    def test_sanitize_inbox(self):
        """Test that INBOX passes through."""
        assert sanitize_folder_name('INBOX') == 'INBOX'


class TestStorage:
    """Tests for Storage class."""
    
    @pytest.fixture
    def storage(self):
        """Create a storage instance with temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Storage(tmpdir)
    
    def test_get_account_path(self, storage):
        """Test account path generation."""
        date = datetime(2024, 1, 15)
        path = storage.get_account_path('test@example.com', date)
        
        assert 'test' in path
        assert '2024' in path
        assert '2024-01-15' in path
    
    def test_get_folder_path(self, storage):
        """Test folder path generation."""
        date = datetime(2024, 1, 15)
        path = storage.get_folder_path('test@example.com', date, 'INBOX')
        
        assert 'INBOX' in path
    
    def test_create_directory_structure(self, storage):
        """Test directory structure creation."""
        date = datetime(2024, 1, 15)
        folders = ['INBOX', 'Posta inviata']
        
        account_path = storage.create_directory_structure(
            'test@example.com',
            date,
            folders
        )
        
        assert os.path.exists(account_path)
        assert os.path.exists(os.path.join(account_path, 'INBOX'))
        assert os.path.exists(os.path.join(account_path, 'Posta_inviata'))
    
    def test_save_eml(self, storage):
        """Test saving .eml file."""
        date = datetime(2024, 1, 15)
        
        # Create test message
        msg = EmailMessage()
        msg['Subject'] = 'Test Subject'
        msg['From'] = 'sender@example.com'
        msg['To'] = 'recipient@example.com'
        msg.set_content('Test body')
        
        raw_email = msg.as_bytes()
        
        # Create directory structure first
        storage.create_directory_structure('test@example.com', date, ['INBOX'])
        
        # Save message
        filepath = storage.save_eml(
            'test@example.com',
            date,
            'INBOX',
            '123',
            msg,
            raw_email
        )
        
        assert os.path.exists(filepath)
        assert filepath.endswith('.eml')
        
        # Verify content
        with open(filepath, 'rb') as f:
            content = f.read()
        assert b'Test Subject' in content
    
    def test_get_saved_messages(self, storage):
        """Test getting list of saved messages."""
        date = datetime(2024, 1, 15)
        
        # Create test message and save it
        msg = EmailMessage()
        msg['Subject'] = 'Test'
        raw_email = msg.as_bytes()
        
        storage.create_directory_structure('test@example.com', date, ['INBOX'])
        storage.save_eml('test@example.com', date, 'INBOX', '1', msg, raw_email)
        storage.save_eml('test@example.com', date, 'INBOX', '2', msg, raw_email)
        
        messages = storage.get_saved_messages('test@example.com', date, 'INBOX')
        assert len(messages) == 2
        assert all(m.endswith('.eml') for m in messages)


class TestStorageWithoutDateFolders:
    """Tests for Storage class without date-based folder structure."""
    
    @pytest.fixture
    def storage_no_dates(self):
        """Create a storage instance without date folders."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Storage(tmpdir, use_date_folders=False)
    
    def test_get_account_path_no_dates(self, storage_no_dates):
        """Test account path generation without date folders."""
        date = datetime(2024, 1, 15)
        path = storage_no_dates.get_account_path('test@example.com', date)
        
        assert 'test' in path
        # Should NOT contain year or date folders
        assert '2024' not in path
        assert '2024-01-15' not in path
        assert path.endswith('test')
    
    def test_get_folder_path_no_dates(self, storage_no_dates):
        """Test folder path generation without date folders."""
        date = datetime(2024, 1, 15)
        path = storage_no_dates.get_folder_path('test@example.com', date, 'INBOX')
        
        assert 'INBOX' in path
        # Should NOT contain year or date folders
        assert '2024' not in path
        assert '2024-01-15' not in path
    
    def test_create_directory_structure_no_dates(self, storage_no_dates):
        """Test directory structure creation without date folders."""
        date = datetime(2024, 1, 15)
        folders = ['INBOX', 'Posta inviata']
        
        account_path = storage_no_dates.create_directory_structure(
            'test@example.com',
            date,
            folders
        )
        
        # Should create account/folder structure directly
        assert os.path.exists(account_path)
        assert os.path.exists(os.path.join(account_path, 'INBOX'))
        assert os.path.exists(os.path.join(account_path, 'Posta_inviata'))
        
        # Verify no date folders exist
        assert '2024' not in account_path
        assert '2024-01-15' not in account_path
    
    def test_save_eml_no_dates(self, storage_no_dates):
        """Test saving .eml file without date folders."""
        date = datetime(2024, 1, 15)
        
        # Create test message
        msg = EmailMessage()
        msg['Subject'] = 'Test Subject'
        msg['From'] = 'sender@example.com'
        msg['To'] = 'recipient@example.com'
        msg.set_content('Test body')
        
        raw_email = msg.as_bytes()
        
        # Create directory structure first
        storage_no_dates.create_directory_structure('test@example.com', date, ['INBOX'])
        
        # Save message
        filepath = storage_no_dates.save_eml(
            'test@example.com',
            date,
            'INBOX',
            '123',
            msg,
            raw_email
        )
        
        assert os.path.exists(filepath)
        assert filepath.endswith('.eml')
        
        # Verify no date folders in path
        assert '2024-01-15' not in filepath
        
        # Verify content
        with open(filepath, 'rb') as f:
            content = f.read()
        assert b'Test Subject' in content
    
    def test_accumulative_storage_no_dates(self, storage_no_dates):
        """Test that emails accumulate without date folders."""
        date1 = datetime(2024, 1, 15)
        date2 = datetime(2024, 1, 16)
        
        msg = EmailMessage()
        msg['Subject'] = 'Test'
        raw_email = msg.as_bytes()
        
        # Create structure for day 1
        storage_no_dates.create_directory_structure('test@example.com', date1, ['INBOX'])
        storage_no_dates.save_eml('test@example.com', date1, 'INBOX', '1', msg, raw_email)
        
        # Add message for day 2 (should go to same folder)
        storage_no_dates.create_directory_structure('test@example.com', date2, ['INBOX'])
        storage_no_dates.save_eml('test@example.com', date2, 'INBOX', '2', msg, raw_email)
        
        # Both messages should be in the same folder
        messages_day1 = storage_no_dates.get_saved_messages('test@example.com', date1, 'INBOX')
        messages_day2 = storage_no_dates.get_saved_messages('test@example.com', date2, 'INBOX')
        
        # Since there are no date folders, both queries should return the same list
        assert len(messages_day1) == 2
        assert len(messages_day2) == 2
        assert set(messages_day1) == set(messages_day2)
