"""
S3 Storage module for PEC Archiver.
Handles uploading daily archives to Amazon S3.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class S3StorageError(Exception):
    """S3 storage operation error."""
    pass


class S3Storage:
    """
    S3 storage handler for daily archives.
    Uploads compressed daily archives to S3 bucket.
    """
    
    def __init__(self, s3_config: dict):
        """
        Initialize S3 storage handler.
        
        Args:
            s3_config: S3 configuration dictionary with bucket, region, credentials
        
        Raises:
            S3StorageError: If boto3 is not available or configuration is invalid
        """
        try:
            import boto3
            from botocore.exceptions import BotoCoreError, ClientError
            self.boto3 = boto3
            self.BotoCoreError = BotoCoreError
            self.ClientError = ClientError
        except ImportError:
            raise S3StorageError(
                "boto3 is not installed. Install it with: pip install boto3"
            )
        
        self.bucket_name = s3_config.get('bucket')
        if not self.bucket_name:
            raise S3StorageError("S3 bucket name is required")
        
        self.region = s3_config.get('region', 'us-east-1')
        self.prefix = s3_config.get('prefix', 'pec-backups')
        
        # Initialize S3 client
        session_kwargs = {'region_name': self.region}
        
        # Use explicit credentials if provided, otherwise use default credential chain
        aws_access_key = s3_config.get('aws_access_key_id')
        aws_secret_key = s3_config.get('aws_secret_access_key')
        
        if aws_access_key and aws_secret_key:
            session_kwargs['aws_access_key_id'] = aws_access_key
            session_kwargs['aws_secret_access_key'] = aws_secret_key
        
        try:
            self.s3_client = self.boto3.client('s3', **session_kwargs)
        except Exception as e:
            raise S3StorageError(f"Failed to initialize S3 client: {e}")
    
    def upload_archive(
        self,
        archive_path: str,
        account: str,
        date: datetime,
        digest_path: Optional[str] = None
    ) -> dict:
        """
        Upload daily archive to S3.
        
        Args:
            archive_path: Path to the archive file (.tar.gz)
            account: Account username (email)
            date: Archive date
            digest_path: Optional path to SHA256 digest file
        
        Returns:
            Dictionary with upload results
        
        Raises:
            S3StorageError: If upload fails
        """
        if not os.path.exists(archive_path):
            raise S3StorageError(f"Archive file not found: {archive_path}")
        
        # Extract account name (part before @)
        account_name = account.split('@')[0]
        
        # Build S3 key path: prefix/account/YYYY/YYYY-MM-DD/filename
        year = date.strftime('%Y')
        date_str = date.strftime('%Y-%m-%d')
        archive_filename = os.path.basename(archive_path)
        s3_key = f"{self.prefix}/{account_name}/{year}/{date_str}/{archive_filename}"
        
        logger.info(f"Uploading archive to S3: s3://{self.bucket_name}/{s3_key}")
        
        try:
            # Upload archive file
            file_size = os.path.getsize(archive_path)
            
            self.s3_client.upload_file(
                archive_path,
                self.bucket_name,
                s3_key,
                ExtraArgs={'StorageClass': 'STANDARD_IA'}  # Use Infrequent Access for cost savings
            )
            
            logger.info(
                f"Successfully uploaded archive: "
                f"s3://{self.bucket_name}/{s3_key} "
                f"({file_size / (1024*1024):.2f} MB)"
            )
            
            result = {
                'success': True,
                'bucket': self.bucket_name,
                's3_key': s3_key,
                'size_bytes': file_size,
                'archive_path': archive_path
            }
            
            # Upload digest file if provided
            if digest_path and os.path.exists(digest_path):
                digest_filename = os.path.basename(digest_path)
                digest_s3_key = f"{self.prefix}/{account_name}/{year}/{date_str}/{digest_filename}"
                
                self.s3_client.upload_file(
                    digest_path,
                    self.bucket_name,
                    digest_s3_key
                )
                
                logger.info(f"Successfully uploaded digest: s3://{self.bucket_name}/{digest_s3_key}")
                result['digest_s3_key'] = digest_s3_key
            
            return result
            
        except self.ClientError as e:
            error_msg = f"S3 upload failed: {e}"
            logger.error(error_msg)
            raise S3StorageError(error_msg)
        except self.BotoCoreError as e:
            error_msg = f"S3 upload failed (BotoCore error): {e}"
            logger.error(error_msg)
            raise S3StorageError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during S3 upload: {e}"
            logger.error(error_msg)
            raise S3StorageError(error_msg)
    
    def verify_bucket_access(self) -> bool:
        """
        Verify that the S3 bucket is accessible.
        
        Returns:
            True if bucket is accessible, False otherwise
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully verified access to S3 bucket: {self.bucket_name}")
            return True
        except self.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                logger.error(f"S3 bucket not found: {self.bucket_name}")
            elif error_code == '403':
                logger.error(f"Access denied to S3 bucket: {self.bucket_name}")
            else:
                logger.error(f"Error accessing S3 bucket: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error verifying S3 bucket: {e}")
            return False


def validate_s3_config(s3_config: dict) -> list[str]:
    """
    Validate S3 configuration.
    
    Args:
        s3_config: S3 configuration dictionary
    
    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    
    if not isinstance(s3_config, dict):
        errors.append("S3 configuration must be a dictionary")
        return errors
    
    # Required fields
    if not s3_config.get('bucket'):
        errors.append("S3 bucket name is required")
    
    # Optional but recommended
    if not s3_config.get('region'):
        errors.append("S3 region is recommended (defaults to us-east-1)")
    
    # Credentials (optional - can use IAM roles or env vars)
    aws_access_key = s3_config.get('aws_access_key_id')
    aws_secret_key = s3_config.get('aws_secret_access_key')
    
    if aws_access_key and not aws_secret_key:
        errors.append("aws_secret_access_key is required when aws_access_key_id is provided")
    elif aws_secret_key and not aws_access_key:
        errors.append("aws_access_key_id is required when aws_secret_access_key is provided")
    
    return errors
