"""
Configuration module for PEC Archiver.
Loads and validates configuration from YAML file.
"""

from __future__ import annotations

import os
import re
import yaml
import logging
from typing import Any

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Configuration error exception."""
    pass


def expand_env_vars(value: Any) -> Any:
    """
    Recursively expand environment variables in configuration values.
    Supports ${VAR_NAME} syntax.
    """
    if isinstance(value, str):
        pattern = re.compile(r'\$\{([^}]+)\}')
        matches = pattern.findall(value)
        for match in matches:
            env_value = os.environ.get(match, '')
            value = value.replace(f'${{{match}}}', env_value)
        return value
    elif isinstance(value, dict):
        return {k: expand_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [expand_env_vars(item) for item in value]
    return value


def load_config(config_path: str = None) -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, uses PEC_ARCHIVE_CONFIG env var.
    
    Returns:
        Configuration dictionary.
    
    Raises:
        ConfigError: If configuration is invalid or file not found.
    """
    if config_path is None:
        config_path = os.environ.get('PEC_ARCHIVE_CONFIG', '/app/config/config.yaml')
    
    if not os.path.exists(config_path):
        raise ConfigError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in configuration file: {e}")
    
    # Expand environment variables
    config = expand_env_vars(config)
    
    # Validate required fields
    validate_config(config)
    
    logger.info(f"Configuration loaded from {config_path}")
    return config


def validate_config(config: dict) -> None:
    """
    Validate configuration dictionary.
    
    Raises:
        ConfigError: If required fields are missing or invalid.
    """
    if not config:
        raise ConfigError("Configuration is empty")
    
    # Required top-level fields
    required_fields = ['base_path', 'accounts']
    for field in required_fields:
        if field not in config:
            raise ConfigError(f"Missing required field: {field}")
    
    # Validate accounts
    if not isinstance(config['accounts'], list) or len(config['accounts']) == 0:
        raise ConfigError("At least one account must be configured")
    
    for i, account in enumerate(config['accounts']):
        required_account_fields = ['username', 'password', 'host', 'folders']
        for field in required_account_fields:
            if field not in account:
                raise ConfigError(f"Account {i+1}: missing required field '{field}'")
        
        if not isinstance(account['folders'], list) or len(account['folders']) == 0:
            raise ConfigError(f"Account {i+1}: at least one folder must be specified")
    
    # Set defaults
    config.setdefault('concurrency', 4)
    config.setdefault('retry_policy', {
        'max_retries': 3,
        'initial_delay': 5,
        'backoff_multiplier': 2
    })
    config.setdefault('imap', {
        'timeout': 30,
        'batch_size': 100
    })
    config.setdefault('scheduler', {
        'run_time': '01:00'
    })
    
    # Set default notifications config (disabled by default)
    config.setdefault('notifications', {
        'enabled': False
    })
    
    # Validate notifications if enabled
    notifications = config.get('notifications', {})
    if notifications.get('enabled', False):
        from .notifications import validate_notification_config
        errors = validate_notification_config(notifications)
        if errors:
            raise ConfigError(f"Invalid notifications configuration: {'; '.join(errors)}")
    
    # Set default backup mode
    config.setdefault('backup_mode', 'standard')
    
    # Validate backup mode
    valid_modes = ['standard', 's3_sync']
    if config['backup_mode'] not in valid_modes:
        raise ConfigError(
            f"Invalid backup_mode: {config['backup_mode']}. "
            f"Must be one of: {', '.join(valid_modes)}"
        )
    
    # Validate S3 configuration if s3_sync mode is enabled
    if config['backup_mode'] == 's3_sync':
        if 's3' not in config:
            raise ConfigError("S3 configuration is required when backup_mode is 's3_sync'")
        
        from .s3_storage import validate_s3_config
        s3_errors = validate_s3_config(config['s3'])
        if s3_errors:
            raise ConfigError(f"Invalid S3 configuration: {'; '.join(s3_errors)}")
    
    # Set defaults for accounts
    for account in config['accounts']:
        account.setdefault('port', 993)


def get_default_config() -> dict:
    """Return default configuration template."""
    return {
        'base_path': '/data/pec-archive',
        'backup_mode': 'standard',
        'concurrency': 4,
        'retry_policy': {
            'max_retries': 3,
            'initial_delay': 5,
            'backoff_multiplier': 2
        },
        'imap': {
            'timeout': 30,
            'batch_size': 100
        },
        'scheduler': {
            'run_time': '01:00'
        },
        'notifications': {
            'enabled': False
        },
        'accounts': []
    }
