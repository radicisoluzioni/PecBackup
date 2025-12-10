"""
Tests for configuration module.
"""

import os
import tempfile
import pytest
import yaml

from src.config import load_config, ConfigError, expand_env_vars, validate_config


class TestExpandEnvVars:
    """Tests for environment variable expansion."""
    
    def test_expand_simple_var(self, monkeypatch):
        """Test expanding a simple environment variable."""
        monkeypatch.setenv('TEST_VAR', 'test_value')
        result = expand_env_vars('${TEST_VAR}')
        assert result == 'test_value'
    
    def test_expand_var_in_string(self, monkeypatch):
        """Test expanding variable within a string."""
        monkeypatch.setenv('USER', 'admin')
        result = expand_env_vars('Hello ${USER}!')
        assert result == 'Hello admin!'
    
    def test_expand_missing_var(self):
        """Test expanding a missing variable returns empty string."""
        result = expand_env_vars('${NONEXISTENT_VAR_12345}')
        assert result == ''
    
    def test_expand_in_dict(self, monkeypatch):
        """Test expanding variables in dictionary."""
        monkeypatch.setenv('PASSWORD', 'secret')
        data = {'password': '${PASSWORD}', 'user': 'admin'}
        result = expand_env_vars(data)
        assert result == {'password': 'secret', 'user': 'admin'}
    
    def test_expand_in_list(self, monkeypatch):
        """Test expanding variables in list."""
        monkeypatch.setenv('ITEM', 'value')
        data = ['${ITEM}', 'static']
        result = expand_env_vars(data)
        assert result == ['value', 'static']


class TestValidateConfig:
    """Tests for configuration validation."""
    
    def test_empty_config_raises_error(self):
        """Test that empty config raises ConfigError."""
        with pytest.raises(ConfigError):
            validate_config({})
    
    def test_missing_base_path_raises_error(self):
        """Test that missing base_path raises ConfigError."""
        config = {'accounts': []}
        with pytest.raises(ConfigError) as exc_info:
            validate_config(config)
        assert 'base_path' in str(exc_info.value)
    
    def test_missing_accounts_raises_error(self):
        """Test that missing accounts raises ConfigError."""
        config = {'base_path': '/data'}
        with pytest.raises(ConfigError) as exc_info:
            validate_config(config)
        assert 'accounts' in str(exc_info.value)
    
    def test_empty_accounts_raises_error(self):
        """Test that empty accounts list raises ConfigError."""
        config = {'base_path': '/data', 'accounts': []}
        with pytest.raises(ConfigError) as exc_info:
            validate_config(config)
        assert 'At least one account' in str(exc_info.value)
    
    def test_account_missing_username_raises_error(self):
        """Test that account missing username raises ConfigError."""
        config = {
            'base_path': '/data',
            'accounts': [{'password': 'secret', 'host': 'imap.example.com', 'folders': ['INBOX']}]
        }
        with pytest.raises(ConfigError) as exc_info:
            validate_config(config)
        assert 'username' in str(exc_info.value)
    
    def test_valid_config_passes(self):
        """Test that valid configuration passes validation."""
        config = {
            'base_path': '/data',
            'accounts': [{
                'username': 'test@example.com',
                'password': 'secret',
                'host': 'imap.example.com',
                'folders': ['INBOX']
            }]
        }
        # Should not raise
        validate_config(config)
        
        # Check defaults are set
        assert config['concurrency'] == 4
        assert 'retry_policy' in config
        assert config['accounts'][0]['port'] == 993
        assert config['backup_mode'] == 'standard'  # Default mode
    
    def test_valid_config_with_s3_sync_mode(self):
        """Test that valid S3 sync configuration passes validation."""
        config = {
            'base_path': '/data',
            'backup_mode': 's3_sync',
            's3': {
                'bucket': 'test-bucket',
                'region': 'us-east-1'
            },
            'accounts': [{
                'username': 'test@example.com',
                'password': 'secret',
                'host': 'imap.example.com',
                'folders': ['INBOX']
            }]
        }
        # Should not raise
        validate_config(config)
        
        assert config['backup_mode'] == 's3_sync'
    
    def test_invalid_backup_mode_raises_error(self):
        """Test that invalid backup mode raises ConfigError."""
        config = {
            'base_path': '/data',
            'backup_mode': 'invalid_mode',
            'accounts': [{
                'username': 'test@example.com',
                'password': 'secret',
                'host': 'imap.example.com',
                'folders': ['INBOX']
            }]
        }
        with pytest.raises(ConfigError) as exc_info:
            validate_config(config)
        assert 'backup_mode' in str(exc_info.value)
    
    def test_s3_sync_without_s3_config_raises_error(self):
        """Test that s3_sync mode without S3 config raises ConfigError."""
        config = {
            'base_path': '/data',
            'backup_mode': 's3_sync',
            'accounts': [{
                'username': 'test@example.com',
                'password': 'secret',
                'host': 'imap.example.com',
                'folders': ['INBOX']
            }]
        }
        with pytest.raises(ConfigError) as exc_info:
            validate_config(config)
        assert 's3' in str(exc_info.value).lower()
    
    def test_s3_sync_with_invalid_s3_config_raises_error(self):
        """Test that s3_sync mode with invalid S3 config raises ConfigError."""
        config = {
            'base_path': '/data',
            'backup_mode': 's3_sync',
            's3': {
                'region': 'us-east-1'
                # Missing required 'bucket'
            },
            'accounts': [{
                'username': 'test@example.com',
                'password': 'secret',
                'host': 'imap.example.com',
                'folders': ['INBOX']
            }]
        }
        with pytest.raises(ConfigError) as exc_info:
            validate_config(config)
        assert 'bucket' in str(exc_info.value).lower()


class TestLoadConfig:
    """Tests for configuration loading."""
    
    def test_load_valid_config(self):
        """Test loading a valid configuration file."""
        config_data = {
            'base_path': '/data/pec-archive',
            'concurrency': 2,
            'accounts': [{
                'username': 'test@pec.it',
                'password': 'secret',
                'host': 'imaps.pec.aruba.it',
                'folders': ['INBOX', 'Posta inviata']
            }]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            f.flush()
            
            config = load_config(f.name)
            
            assert config['base_path'] == '/data/pec-archive'
            assert config['concurrency'] == 2
            assert len(config['accounts']) == 1
        
        os.unlink(f.name)
    
    def test_load_nonexistent_file_raises_error(self):
        """Test that loading nonexistent file raises ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            load_config('/nonexistent/path/config.yaml')
        assert 'not found' in str(exc_info.value)
    
    def test_load_invalid_yaml_raises_error(self):
        """Test that invalid YAML raises ConfigError."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            f.flush()
            
            with pytest.raises(ConfigError) as exc_info:
                load_config(f.name)
            assert 'Invalid YAML' in str(exc_info.value)
        
        os.unlink(f.name)
