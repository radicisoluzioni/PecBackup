"""
Tests for indexing module.
"""

import os
import tempfile
import json
import csv
import pytest
from datetime import datetime
from email.message import EmailMessage

from src.indexing import (
    Indexer,
    decode_email_header,
    parse_email_date,
    extract_message_info
)


class TestDecodeEmailHeader:
    """Tests for email header decoding."""
    
    def test_decode_simple_header(self):
        """Test decoding simple ASCII header."""
        result = decode_email_header('Hello World')
        assert result == 'Hello World'
    
    def test_decode_none_header(self):
        """Test decoding None header."""
        result = decode_email_header(None)
        assert result == ''
    
    def test_decode_empty_header(self):
        """Test decoding empty header."""
        result = decode_email_header('')
        assert result == ''


class TestParseEmailDate:
    """Tests for email date parsing."""
    
    def test_parse_valid_date(self):
        """Test parsing valid email date."""
        date_str = 'Mon, 15 Jan 2024 10:30:00 +0100'
        result = parse_email_date(date_str)
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
    
    def test_parse_none_date(self):
        """Test parsing None date."""
        result = parse_email_date(None)
        assert result is None
    
    def test_parse_invalid_date(self):
        """Test parsing invalid date."""
        result = parse_email_date('not a date')
        assert result is None


class TestExtractMessageInfo:
    """Tests for message info extraction."""
    
    def test_extract_basic_info(self):
        """Test extracting basic message info."""
        msg = EmailMessage()
        msg['Subject'] = 'Test Subject'
        msg['From'] = 'sender@example.com'
        msg['To'] = 'recipient@example.com'
        msg['Date'] = 'Mon, 15 Jan 2024 10:30:00 +0100'
        msg['Message-ID'] = '<12345@example.com>'
        
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False) as f:
            f.write(msg.as_bytes())
            f.flush()
            
            info = extract_message_info(msg, '123', 'INBOX', f.name)
            
            assert info['uid'] == '123'
            assert info['folder'] == 'INBOX'
            assert info['subject'] == 'Test Subject'
            assert info['from'] == 'sender@example.com'
            assert info['to'] == 'recipient@example.com'
            assert info['message_id'] == '<12345@example.com>'
            assert info['size'] > 0
        
        os.unlink(f.name)
    
    def test_extract_message_with_unread_flag(self):
        """Test extracting message info with unread flag."""
        msg = EmailMessage()
        msg['Subject'] = 'Unread Message'
        msg['From'] = 'sender@example.com'
        
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False) as f:
            f.write(msg.as_bytes())
            f.flush()
            
            # Test with unread flag
            info = extract_message_info(msg, '123', 'INBOX', f.name, is_unread=True)
            assert info['is_unread'] is True
            
            # Test with read flag
            info = extract_message_info(msg, '124', 'INBOX', f.name, is_unread=False)
            assert info['is_unread'] is False
        
        os.unlink(f.name)


class TestIndexer:
    """Tests for Indexer class."""
    
    @pytest.fixture
    def indexer(self):
        """Create an indexer with temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Indexer(tmpdir)
    
    def test_add_message(self, indexer):
        """Test adding a message to index."""
        msg = EmailMessage()
        msg['Subject'] = 'Test'
        
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False, dir=indexer.account_path) as f:
            f.write(msg.as_bytes())
            f.flush()
            
            indexer.add_message(msg, '1', 'INBOX', f.name)
            
            assert len(indexer.messages) == 1
            assert indexer.messages[0]['uid'] == '1'
        
        os.unlink(f.name)
    
    def test_add_message_with_unread_flag(self, indexer):
        """Test adding a message with unread flag to index."""
        msg = EmailMessage()
        msg['Subject'] = 'Unread Test'
        
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False, dir=indexer.account_path) as f:
            f.write(msg.as_bytes())
            f.flush()
            
            # Add unread message
            indexer.add_message(msg, '1', 'INBOX', f.name, is_unread=True)
            
            assert len(indexer.messages) == 1
            assert indexer.messages[0]['uid'] == '1'
            assert indexer.messages[0]['is_unread'] is True
            
            # Add read message
            indexer.add_message(msg, '2', 'INBOX', f.name, is_unread=False)
            
            assert len(indexer.messages) == 2
            assert indexer.messages[1]['uid'] == '2'
            assert indexer.messages[1]['is_unread'] is False
        
        os.unlink(f.name)
    
    def test_generate_csv(self, indexer):
        """Test CSV generation."""
        msg = EmailMessage()
        msg['Subject'] = 'Test Subject'
        msg['From'] = 'sender@example.com'
        
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False, dir=indexer.account_path) as f:
            f.write(msg.as_bytes())
            f.flush()
            
            indexer.add_message(msg, '1', 'INBOX', f.name)
            csv_path = indexer.generate_csv()
            
            assert os.path.exists(csv_path)
            
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                assert len(rows) == 1
                assert rows[0]['uid'] == '1'
                assert rows[0]['subject'] == 'Test Subject'
        
        os.unlink(f.name)
    
    def test_generate_csv_with_unread_flag(self, indexer):
        """Test CSV generation with unread flag."""
        msg = EmailMessage()
        msg['Subject'] = 'Test Subject'
        msg['From'] = 'sender@example.com'
        
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False, dir=indexer.account_path) as f:
            f.write(msg.as_bytes())
            f.flush()
            
            indexer.add_message(msg, '1', 'INBOX', f.name, is_unread=True)
            csv_path = indexer.generate_csv()
            
            assert os.path.exists(csv_path)
            
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                assert len(rows) == 1
                assert rows[0]['uid'] == '1'
                assert rows[0]['is_unread'] == 'True'
                
            # Verify the is_unread field is in the header
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)
                assert 'is_unread' in headers
        
        os.unlink(f.name)
    
    def test_generate_json(self, indexer):
        """Test JSON generation."""
        msg = EmailMessage()
        msg['Subject'] = 'Test Subject'
        
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False, dir=indexer.account_path) as f:
            f.write(msg.as_bytes())
            f.flush()
            
            indexer.add_message(msg, '1', 'INBOX', f.name)
            json_path = indexer.generate_json()
            
            assert os.path.exists(json_path)
            
            with open(json_path, 'r', encoding='utf-8') as jsonfile:
                data = json.load(jsonfile)
                assert len(data) == 1
                assert data[0]['uid'] == '1'
                assert data[0]['subject'] == 'Test Subject'
        
        os.unlink(f.name)
    
    def test_generate_json_with_unread_flag(self, indexer):
        """Test JSON generation with unread flag."""
        msg = EmailMessage()
        msg['Subject'] = 'Test Subject'
        
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False, dir=indexer.account_path) as f:
            f.write(msg.as_bytes())
            f.flush()
            
            indexer.add_message(msg, '1', 'INBOX', f.name, is_unread=True)
            json_path = indexer.generate_json()
            
            assert os.path.exists(json_path)
            
            with open(json_path, 'r', encoding='utf-8') as jsonfile:
                data = json.load(jsonfile)
                assert len(data) == 1
                assert data[0]['uid'] == '1'
                assert data[0]['is_unread'] is True
        
        os.unlink(f.name)
    
    def test_get_stats(self, indexer):
        """Test statistics calculation."""
        msg1 = EmailMessage()
        msg1['Subject'] = 'Test 1'
        
        msg2 = EmailMessage()
        msg2['Subject'] = 'Test 2'
        
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False, dir=indexer.account_path) as f1:
            f1.write(msg1.as_bytes())
            f1.flush()
            
            with tempfile.NamedTemporaryFile(suffix='.eml', delete=False, dir=indexer.account_path) as f2:
                f2.write(msg2.as_bytes())
                f2.flush()
                
                indexer.add_message(msg1, '1', 'INBOX', f1.name)
                indexer.add_message(msg2, '2', 'Posta inviata', f2.name)
                
                stats = indexer.get_stats()
                
                assert stats['total_messages'] == 2
                assert stats['folders']['INBOX'] == 1
                assert stats['folders']['Posta inviata'] == 1
            
            os.unlink(f2.name)
        os.unlink(f1.name)
