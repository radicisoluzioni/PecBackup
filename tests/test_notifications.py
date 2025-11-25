"""
Tests for notifications module.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from src.notifications import (
    format_report_html,
    format_report_text,
    send_notification,
    validate_notification_config,
    NotificationError
)


class TestFormatReportText:
    """Tests for text report formatting."""
    
    def test_format_success_report(self):
        """Test formatting a successful report."""
        report = {
            'accounts_processed': 2,
            'accounts_successful': 2,
            'accounts_with_errors': 0,
            'total_messages': 150,
            'total_errors': 0,
            'accounts': [
                {'account': 'test1@pec.it', 'status': 'success', 'messages': 100},
                {'account': 'test2@pec.it', 'status': 'success', 'messages': 50}
            ]
        }
        target_date = datetime(2024, 1, 15)
        
        result = format_report_text(report, target_date)
        
        assert 'SUCCESS' in result
        assert '2024-01-15' in result
        assert 'Account processati:  2' in result
        assert 'Account con successo: 2' in result
        assert 'Messaggi totali:     150' in result
        assert 'test1@pec.it' in result
        assert 'test2@pec.it' in result
    
    def test_format_error_report(self):
        """Test formatting a report with errors."""
        report = {
            'accounts_processed': 2,
            'accounts_successful': 1,
            'accounts_with_errors': 1,
            'total_messages': 100,
            'total_errors': 3,
            'accounts': [
                {'account': 'test1@pec.it', 'status': 'success', 'messages': 100},
                {'account': 'test2@pec.it', 'status': 'error', 'messages': 0}
            ]
        }
        target_date = datetime(2024, 1, 15)
        
        result = format_report_text(report, target_date)
        
        assert 'COMPLETED WITH ERRORS' in result
        assert 'Account con errori:  1' in result
        assert 'Errori totali:       3' in result
        assert '[OK] test1@pec.it' in result
        assert '[ERROR] test2@pec.it' in result


class TestFormatReportHtml:
    """Tests for HTML report formatting."""
    
    def test_format_html_report(self):
        """Test formatting HTML report."""
        report = {
            'accounts_processed': 1,
            'accounts_successful': 1,
            'accounts_with_errors': 0,
            'total_messages': 50,
            'total_errors': 0,
            'accounts': [
                {'account': 'test@pec.it', 'status': 'success', 'messages': 50}
            ]
        }
        target_date = datetime(2024, 1, 15)
        
        result = format_report_html(report, target_date)
        
        assert '<html>' in result
        assert '2024-01-15' in result
        assert 'SUCCESS' in result
        assert 'test@pec.it' in result
    
    def test_format_html_error_report(self):
        """Test formatting HTML report with errors."""
        report = {
            'accounts_processed': 1,
            'accounts_successful': 0,
            'accounts_with_errors': 1,
            'total_messages': 0,
            'total_errors': 1
        }
        target_date = datetime(2024, 1, 15)
        
        result = format_report_html(report, target_date)
        
        assert 'ERRORS' in result
        assert 'class="header error"' in result


class TestValidateNotificationConfig:
    """Tests for notification configuration validation."""
    
    def test_disabled_config_is_valid(self):
        """Test that disabled config passes validation."""
        config = {'enabled': False}
        errors = validate_notification_config(config)
        assert errors == []
    
    def test_valid_enabled_config(self):
        """Test that valid enabled config passes validation."""
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'smtp': {
                'host': 'smtp.example.com',
                'username': 'user',
                'password': 'pass'
            }
        }
        errors = validate_notification_config(config)
        assert errors == []
    
    def test_missing_recipients(self):
        """Test that missing recipients is detected."""
        config = {
            'enabled': True,
            'smtp': {
                'host': 'smtp.example.com',
                'username': 'user',
                'password': 'pass'
            }
        }
        errors = validate_notification_config(config)
        assert any('recipients' in e for e in errors)
    
    def test_missing_smtp_host(self):
        """Test that missing SMTP host is detected."""
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'smtp': {
                'username': 'user',
                'password': 'pass'
            }
        }
        errors = validate_notification_config(config)
        assert any('host' in e for e in errors)
    
    def test_missing_smtp_username(self):
        """Test that missing SMTP username is detected."""
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'smtp': {
                'host': 'smtp.example.com',
                'password': 'pass'
            }
        }
        errors = validate_notification_config(config)
        assert any('username' in e for e in errors)
    
    def test_missing_smtp_password(self):
        """Test that missing SMTP password is detected."""
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'smtp': {
                'host': 'smtp.example.com',
                'username': 'user'
            }
        }
        errors = validate_notification_config(config)
        assert any('password' in e for e in errors)
    
    def test_invalid_send_on_value(self):
        """Test that invalid send_on value is detected."""
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'send_on': 'invalid',
            'smtp': {
                'host': 'smtp.example.com',
                'username': 'user',
                'password': 'pass'
            }
        }
        errors = validate_notification_config(config)
        assert any('send_on' in e for e in errors)
    
    def test_single_recipient_string(self):
        """Test that single recipient as string is valid."""
        config = {
            'enabled': True,
            'recipients': 'admin@example.com',
            'smtp': {
                'host': 'smtp.example.com',
                'username': 'user',
                'password': 'pass'
            }
        }
        errors = validate_notification_config(config)
        assert errors == []
    
    def test_multiple_recipients_list(self):
        """Test that multiple recipients as list is valid."""
        config = {
            'enabled': True,
            'recipients': ['admin1@example.com', 'admin2@example.com'],
            'smtp': {
                'host': 'smtp.example.com',
                'username': 'user',
                'password': 'pass'
            }
        }
        errors = validate_notification_config(config)
        assert errors == []


class TestSendNotification:
    """Tests for send_notification function."""
    
    def test_disabled_notifications_returns_false(self):
        """Test that disabled notifications returns False."""
        config = {'enabled': False}
        report = {'accounts_processed': 1}
        target_date = datetime(2024, 1, 15)
        
        result = send_notification(config, report, target_date)
        assert result is False
    
    def test_no_recipients_returns_false(self):
        """Test that empty recipients returns False."""
        config = {'enabled': True, 'recipients': []}
        report = {'accounts_processed': 1}
        target_date = datetime(2024, 1, 15)
        
        result = send_notification(config, report, target_date)
        assert result is False
    
    def test_incomplete_smtp_returns_false(self):
        """Test that incomplete SMTP config returns False."""
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'smtp': {}
        }
        report = {'accounts_processed': 1}
        target_date = datetime(2024, 1, 15)
        
        result = send_notification(config, report, target_date)
        assert result is False
    
    def test_send_on_error_skips_success(self):
        """Test that send_on='error' skips successful backups."""
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'send_on': 'error',
            'smtp': {
                'host': 'smtp.example.com',
                'username': 'user',
                'password': 'pass'
            }
        }
        report = {
            'accounts_processed': 1,
            'accounts_with_errors': 0,
            'total_errors': 0
        }
        target_date = datetime(2024, 1, 15)
        
        result = send_notification(config, report, target_date)
        assert result is False
    
    @patch('src.notifications.smtplib.SMTP')
    def test_successful_send(self, mock_smtp_class):
        """Test successful notification sending."""
        # Setup mock
        mock_server = MagicMock()
        mock_smtp_class.return_value = mock_server
        
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'smtp': {
                'host': 'smtp.example.com',
                'port': 587,
                'username': 'user',
                'password': 'pass',
                'use_tls': True
            }
        }
        report = {
            'accounts_processed': 1,
            'accounts_successful': 1,
            'accounts_with_errors': 0,
            'total_messages': 50,
            'total_errors': 0
        }
        target_date = datetime(2024, 1, 15)
        
        result = send_notification(config, report, target_date)
        
        assert result is True
        mock_smtp_class.assert_called_once_with('smtp.example.com', 587, timeout=30)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with('user', 'pass')
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()
    
    @patch('src.notifications.smtplib.SMTP')
    def test_send_with_errors_report(self, mock_smtp_class):
        """Test sending notification for report with errors."""
        mock_server = MagicMock()
        mock_smtp_class.return_value = mock_server
        
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'send_on': 'error',
            'smtp': {
                'host': 'smtp.example.com',
                'port': 587,
                'username': 'user',
                'password': 'pass',
                'use_tls': True
            }
        }
        report = {
            'accounts_processed': 1,
            'accounts_successful': 0,
            'accounts_with_errors': 1,
            'total_messages': 0,
            'total_errors': 5
        }
        target_date = datetime(2024, 1, 15)
        
        result = send_notification(config, report, target_date)
        
        assert result is True
        mock_server.sendmail.assert_called_once()
    
    @patch('src.notifications.smtplib.SMTP')
    def test_smtp_error_raises_notification_error(self, mock_smtp_class):
        """Test that SMTP errors raise NotificationError."""
        import smtplib
        mock_smtp_class.side_effect = smtplib.SMTPException("Connection failed")
        
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'smtp': {
                'host': 'smtp.example.com',
                'port': 587,
                'username': 'user',
                'password': 'pass',
                'use_tls': True
            }
        }
        report = {'accounts_processed': 1}
        target_date = datetime(2024, 1, 15)
        
        with pytest.raises(NotificationError):
            send_notification(config, report, target_date)
    
    @patch('src.notifications.smtplib.SMTP_SSL')
    def test_send_with_ssl(self, mock_smtp_ssl_class):
        """Test sending with SSL (use_tls=False)."""
        mock_server = MagicMock()
        mock_smtp_ssl_class.return_value = mock_server
        
        config = {
            'enabled': True,
            'recipients': ['admin@example.com'],
            'smtp': {
                'host': 'smtp.example.com',
                'port': 465,
                'username': 'user',
                'password': 'pass',
                'use_tls': False
            }
        }
        report = {'accounts_processed': 1, 'accounts_with_errors': 0, 'total_errors': 0}
        target_date = datetime(2024, 1, 15)
        
        result = send_notification(config, report, target_date)
        
        assert result is True
        mock_smtp_ssl_class.assert_called_once_with('smtp.example.com', 465, timeout=30)
        mock_server.starttls.assert_not_called()
    
    @patch('src.notifications.smtplib.SMTP')
    def test_multiple_recipients(self, mock_smtp_class):
        """Test sending to multiple recipients."""
        mock_server = MagicMock()
        mock_smtp_class.return_value = mock_server
        
        config = {
            'enabled': True,
            'recipients': ['admin1@example.com', 'admin2@example.com', 'admin3@example.com'],
            'smtp': {
                'host': 'smtp.example.com',
                'port': 587,
                'username': 'user',
                'password': 'pass',
                'use_tls': True
            }
        }
        report = {'accounts_processed': 1, 'accounts_with_errors': 0, 'total_errors': 0}
        target_date = datetime(2024, 1, 15)
        
        result = send_notification(config, report, target_date)
        
        assert result is True
        # Check that sendmail was called with all recipients
        call_args = mock_server.sendmail.call_args
        recipients = call_args[0][1]
        assert len(recipients) == 3
        assert 'admin1@example.com' in recipients
        assert 'admin2@example.com' in recipients
        assert 'admin3@example.com' in recipients
    
    @patch('src.notifications.smtplib.SMTP')
    def test_single_recipient_string(self, mock_smtp_class):
        """Test sending to single recipient as string."""
        mock_server = MagicMock()
        mock_smtp_class.return_value = mock_server
        
        config = {
            'enabled': True,
            'recipients': 'admin@example.com',
            'smtp': {
                'host': 'smtp.example.com',
                'port': 587,
                'username': 'user',
                'password': 'pass',
                'use_tls': True
            }
        }
        report = {'accounts_processed': 1, 'accounts_with_errors': 0, 'total_errors': 0}
        target_date = datetime(2024, 1, 15)
        
        result = send_notification(config, report, target_date)
        
        assert result is True
        call_args = mock_server.sendmail.call_args
        recipients = call_args[0][1]
        assert recipients == ['admin@example.com']
