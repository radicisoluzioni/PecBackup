"""
Tests for S3 storage module.
"""

import os
import sys
import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from src.s3_storage import S3Storage, S3StorageError, validate_s3_config


class TestS3StorageValidation:
    """Tests for S3 configuration validation."""
    
    def test_validate_s3_config_valid(self):
        """Test validation with valid S3 config."""
        config = {
            'bucket': 'my-bucket',
            'region': 'us-east-1'
        }
        errors = validate_s3_config(config)
        assert len(errors) == 0
    
    def test_validate_s3_config_missing_bucket(self):
        """Test validation with missing bucket."""
        config = {
            'region': 'us-east-1'
        }
        errors = validate_s3_config(config)
        assert len(errors) > 0
        assert any('bucket' in error.lower() for error in errors)
    
    def test_validate_s3_config_missing_region(self):
        """Test validation with missing region (should warn, not error)."""
        config = {
            'bucket': 'my-bucket'
        }
        errors = validate_s3_config(config)
        assert any('region' in error.lower() for error in errors)
    
    def test_validate_s3_config_incomplete_credentials(self):
        """Test validation with incomplete credentials."""
        config = {
            'bucket': 'my-bucket',
            'region': 'us-east-1',
            'aws_access_key_id': 'AKIAIOSFODNN7EXAMPLE'
            # Missing aws_secret_access_key
        }
        errors = validate_s3_config(config)
        assert len(errors) > 0
        assert any('secret' in error.lower() for error in errors)
    
    def test_validate_s3_config_invalid_type(self):
        """Test validation with invalid config type."""
        config = "not a dict"
        errors = validate_s3_config(config)
        assert len(errors) > 0


class TestS3Storage:
    """Tests for S3Storage class."""
    
    @pytest.fixture
    def s3_config(self):
        """Fixture for S3 configuration."""
        return {
            'bucket': 'test-bucket',
            'region': 'us-east-1',
            'prefix': 'pec-backups'
        }
    
    @pytest.fixture
    def mock_boto3(self):
        """Fixture for mocked boto3."""
        # Mock boto3 at import time within S3Storage.__init__
        mock_boto3_module = MagicMock()
        mock_client = MagicMock()
        mock_boto3_module.client.return_value = mock_client
        
        # Also set up the exception classes
        from botocore.exceptions import BotoCoreError, ClientError
        mock_boto3_module.BotoCoreError = BotoCoreError
        mock_boto3_module.ClientError = ClientError
        
        with patch.dict('sys.modules', {'boto3': mock_boto3_module}):
            yield mock_boto3_module
    
    def test_init_with_valid_config(self, s3_config, mock_boto3):
        """Test initialization with valid configuration."""
        storage = S3Storage(s3_config)
        assert storage.bucket_name == 'test-bucket'
        assert storage.region == 'us-east-1'
        assert storage.prefix == 'pec-backups'
    
    def test_init_without_bucket(self, mock_boto3):
        """Test initialization without bucket name."""
        config = {'region': 'us-east-1'}
        with pytest.raises(S3StorageError):
            S3Storage(config)
    
    def test_init_with_credentials(self, s3_config, mock_boto3):
        """Test initialization with explicit credentials."""
        config = {
            **s3_config,
            'aws_access_key_id': 'AKIAIOSFODNN7EXAMPLE',
            'aws_secret_access_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        }
        storage = S3Storage(config)
        
        # Verify boto3.client was called with credentials
        mock_boto3.client.assert_called_once()
        call_kwargs = mock_boto3.client.call_args[1]
        assert 'aws_access_key_id' in call_kwargs
        assert 'aws_secret_access_key' in call_kwargs
    
    def test_init_with_endpoint_url(self, s3_config, mock_boto3):
        """Test initialization with custom endpoint URL for S3-compatible services."""
        config = {
            **s3_config,
            'endpoint_url': 'https://my-bucket.s3.eu-central-1.hetzner.cloud',
            'aws_access_key_id': 'AKIAIOSFODNN7EXAMPLE',
            'aws_secret_access_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        }
        storage = S3Storage(config)
        
        # Verify boto3.client was called with endpoint_url
        mock_boto3.client.assert_called_once()
        call_kwargs = mock_boto3.client.call_args[1]
        assert 'endpoint_url' in call_kwargs
        assert call_kwargs['endpoint_url'] == 'https://my-bucket.s3.eu-central-1.hetzner.cloud'
        assert storage.endpoint_url == 'https://my-bucket.s3.eu-central-1.hetzner.cloud'
    
    def test_init_without_endpoint_url(self, s3_config, mock_boto3):
        """Test initialization without endpoint URL for standard AWS S3."""
        storage = S3Storage(s3_config)
        
        # Verify boto3.client was called without endpoint_url
        mock_boto3.client.assert_called_once()
        call_kwargs = mock_boto3.client.call_args[1]
        assert 'endpoint_url' not in call_kwargs
        assert storage.endpoint_url is None
    
    def test_upload_archive_success(self, s3_config, mock_boto3, tmp_path):
        """Test successful archive upload."""
        storage = S3Storage(s3_config)
        
        # Create a test archive file
        archive_path = tmp_path / "archive-test-2024-01-15.tar.gz"
        archive_path.write_bytes(b"test archive content")
        
        # Create a test digest file
        digest_path = tmp_path / "digest.sha256"
        digest_path.write_text("abc123  archive-test-2024-01-15.tar.gz\n")
        
        date = datetime(2024, 1, 15)
        
        result = storage.upload_archive(
            archive_path=str(archive_path),
            account='test@pec.it',
            date=date,
            digest_path=str(digest_path)
        )
        
        assert result['success'] is True
        assert result['bucket'] == 'test-bucket'
        assert 'test/2024/2024-01-15' in result['s3_key']
        assert result['size_bytes'] == len(b"test archive content")
        
        # Verify upload_file was called twice (archive + digest)
        assert storage.s3_client.upload_file.call_count == 2
    
    def test_upload_archive_missing_file(self, s3_config, mock_boto3):
        """Test upload with missing archive file."""
        storage = S3Storage(s3_config)
        
        date = datetime(2024, 1, 15)
        
        with pytest.raises(S3StorageError):
            storage.upload_archive(
                archive_path="/nonexistent/file.tar.gz",
                account='test@pec.it',
                date=date
            )
    
    def test_upload_archive_client_error(self, s3_config, mock_boto3, tmp_path):
        """Test upload with S3 client error."""
        storage = S3Storage(s3_config)
        
        # Mock ClientError
        from botocore.exceptions import ClientError
        error = ClientError(
            {'Error': {'Code': '403', 'Message': 'Access Denied'}},
            'PutObject'
        )
        storage.s3_client.upload_file.side_effect = error
        
        # Create a test archive file
        archive_path = tmp_path / "archive-test-2024-01-15.tar.gz"
        archive_path.write_bytes(b"test archive content")
        
        date = datetime(2024, 1, 15)
        
        with pytest.raises(S3StorageError):
            storage.upload_archive(
                archive_path=str(archive_path),
                account='test@pec.it',
                date=date
            )
    
    def test_verify_bucket_access_success(self, s3_config, mock_boto3):
        """Test successful bucket access verification."""
        storage = S3Storage(s3_config)
        
        # Mock successful head_bucket
        storage.s3_client.head_bucket.return_value = {}
        
        result = storage.verify_bucket_access()
        assert result is True
        storage.s3_client.head_bucket.assert_called_once_with(Bucket='test-bucket')
    
    def test_verify_bucket_access_not_found(self, s3_config, mock_boto3):
        """Test bucket access verification with 404 error."""
        storage = S3Storage(s3_config)
        
        # Mock ClientError with 404
        from botocore.exceptions import ClientError
        error = ClientError(
            {'Error': {'Code': '404', 'Message': 'Not Found'}},
            'HeadBucket'
        )
        storage.s3_client.head_bucket.side_effect = error
        
        result = storage.verify_bucket_access()
        assert result is False
    
    def test_verify_bucket_access_forbidden(self, s3_config, mock_boto3):
        """Test bucket access verification with 403 error."""
        storage = S3Storage(s3_config)
        
        # Mock ClientError with 403
        from botocore.exceptions import ClientError
        error = ClientError(
            {'Error': {'Code': '403', 'Message': 'Forbidden'}},
            'HeadBucket'
        )
        storage.s3_client.head_bucket.side_effect = error
        
        result = storage.verify_bucket_access()
        assert result is False
    
    def test_s3_key_path_format(self, s3_config, mock_boto3, tmp_path):
        """Test that S3 key path follows expected format."""
        storage = S3Storage(s3_config)
        
        # Create a test archive file
        archive_path = tmp_path / "archive-account1-2024-01-15.tar.gz"
        archive_path.write_bytes(b"test")
        
        date = datetime(2024, 1, 15)
        
        result = storage.upload_archive(
            archive_path=str(archive_path),
            account='account1@pec.it',
            date=date
        )
        
        # Expected format: prefix/account/YYYY/YYYY-MM-DD/filename
        expected_key = 'pec-backups/account1/2024/2024-01-15/archive-account1-2024-01-15.tar.gz'
        assert result['s3_key'] == expected_key


class TestS3StorageWithoutBoto3:
    """Tests for S3Storage when boto3 is not available."""
    
    def test_init_without_boto3(self):
        """Test that appropriate error is raised when boto3 is not installed."""
        s3_config = {
            'bucket': 'test-bucket',
            'region': 'us-east-1'
        }
        
        # Temporarily remove boto3 from sys.modules if it exists
        import sys
        boto3_backup = sys.modules.get('boto3')
        
        try:
            # Simulate boto3 not being installed
            if 'boto3' in sys.modules:
                del sys.modules['boto3']
            
            # Need to patch the import to raise ImportError
            with patch.dict('sys.modules', {'boto3': None}):
                with pytest.raises(S3StorageError) as exc_info:
                    S3Storage(s3_config)
                
                assert 'boto3' in str(exc_info.value).lower()
        finally:
            # Restore boto3 if it was there
            if boto3_backup is not None:
                sys.modules['boto3'] = boto3_backup
