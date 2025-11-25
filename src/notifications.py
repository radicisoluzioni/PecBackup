"""
Notifications module for PEC Archiver.
Sends email notifications with daily backup reports and error alerts.
"""

from __future__ import annotations

import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class NotificationError(Exception):
    """Notification operation error."""
    pass


def format_report_html(report: dict, target_date: datetime) -> str:
    """
    Format report as HTML for email.
    
    Args:
        report: Aggregated report dictionary
        target_date: Date that was archived
    
    Returns:
        HTML formatted string
    """
    status = "✅ SUCCESS" if report.get('accounts_with_errors', 0) == 0 else "⚠️ COMPLETED WITH ERRORS"
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .header {{ background-color: #2e7d32; color: white; padding: 15px; border-radius: 5px; }}
            .header.error {{ background-color: #c62828; }}
            .stats {{ margin: 20px 0; }}
            .stats table {{ border-collapse: collapse; width: 100%; max-width: 500px; }}
            .stats td, .stats th {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
            .stats th {{ background-color: #f5f5f5; }}
            .accounts {{ margin: 20px 0; }}
            .accounts table {{ border-collapse: collapse; width: 100%; }}
            .accounts td, .accounts th {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            .accounts th {{ background-color: #1976d2; color: white; }}
            .success {{ color: #2e7d32; }}
            .error {{ color: #c62828; }}
            .footer {{ margin-top: 30px; color: #666; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="header {'error' if report.get('accounts_with_errors', 0) > 0 else ''}">
            <h2>📧 PEC Archiver - Report Giornaliero</h2>
            <p>Data archiviazione: <strong>{target_date.strftime('%Y-%m-%d')}</strong></p>
            <p>Stato: <strong>{status}</strong></p>
        </div>
        
        <div class="stats">
            <h3>📊 Statistiche</h3>
            <table>
                <tr><th>Metrica</th><th>Valore</th></tr>
                <tr><td>Account processati</td><td>{report.get('accounts_processed', 0)}</td></tr>
                <tr><td>Account con successo</td><td class="success">{report.get('accounts_successful', 0)}</td></tr>
                <tr><td>Account con errori</td><td class="{'error' if report.get('accounts_with_errors', 0) > 0 else ''}">{report.get('accounts_with_errors', 0)}</td></tr>
                <tr><td>Messaggi totali</td><td>{report.get('total_messages', 0)}</td></tr>
                <tr><td>Errori totali</td><td class="{'error' if report.get('total_errors', 0) > 0 else ''}">{report.get('total_errors', 0)}</td></tr>
            </table>
        </div>
    """
    
    # Add accounts table if available
    accounts = report.get('accounts', [])
    if accounts:
        html += """
        <div class="accounts">
            <h3>📋 Dettaglio Account</h3>
            <table>
                <tr><th>Account</th><th>Stato</th><th>Messaggi</th></tr>
        """
        for account in accounts:
            status_class = "success" if account.get('status') == 'success' else "error"
            status_text = "✓ Successo" if account.get('status') == 'success' else "✗ Errori"
            html += f"""
                <tr>
                    <td>{account.get('account', 'N/A')}</td>
                    <td class="{status_class}">{status_text}</td>
                    <td>{account.get('messages', 0)}</td>
                </tr>
            """
        html += """
            </table>
        </div>
        """
    
    html += f"""
        <div class="footer">
            <p>Report generato automaticamente da PEC Archiver</p>
            <p>Timestamp: {datetime.now().isoformat()}</p>
        </div>
    </body>
    </html>
    """
    
    return html


def format_report_text(report: dict, target_date: datetime) -> str:
    """
    Format report as plain text for email.
    
    Args:
        report: Aggregated report dictionary
        target_date: Date that was archived
    
    Returns:
        Plain text formatted string
    """
    status = "SUCCESS" if report.get('accounts_with_errors', 0) == 0 else "COMPLETED WITH ERRORS"
    
    lines = [
        "=" * 60,
        "PEC ARCHIVER - REPORT GIORNALIERO",
        "=" * 60,
        "",
        f"Data archiviazione: {target_date.strftime('%Y-%m-%d')}",
        f"Stato: {status}",
        "",
        "-" * 40,
        "STATISTICHE",
        "-" * 40,
        f"Account processati:  {report.get('accounts_processed', 0)}",
        f"Account con successo: {report.get('accounts_successful', 0)}",
        f"Account con errori:  {report.get('accounts_with_errors', 0)}",
        f"Messaggi totali:     {report.get('total_messages', 0)}",
        f"Errori totali:       {report.get('total_errors', 0)}",
        "",
    ]
    
    # Add accounts details if available
    accounts = report.get('accounts', [])
    if accounts:
        lines.append("-" * 40)
        lines.append("DETTAGLIO ACCOUNT")
        lines.append("-" * 40)
        for account in accounts:
            status_text = "[OK]" if account.get('status') == 'success' else "[ERROR]"
            lines.append(f"  {status_text} {account.get('account', 'N/A')} - {account.get('messages', 0)} messaggi")
        lines.append("")
    
    lines.append("=" * 60)
    lines.append(f"Report generato: {datetime.now().isoformat()}")
    lines.append("=" * 60)
    
    return "\n".join(lines)


def send_notification(
    config: dict,
    report: dict,
    target_date: datetime,
    force_send: bool = False
) -> bool:
    """
    Send email notification with backup report.
    
    Args:
        config: Notification configuration dictionary
        report: Aggregated report dictionary
        target_date: Date that was archived
        force_send: Force sending even if notifications are disabled
    
    Returns:
        True if notification was sent successfully, False otherwise
    
    Raises:
        NotificationError: If sending fails
    """
    # Check if notifications are enabled
    if not config.get('enabled', False) and not force_send:
        logger.debug("Notifications are disabled")
        return False
    
    # Get recipients
    recipients = config.get('recipients', [])
    if not recipients:
        logger.warning("No notification recipients configured")
        return False
    
    # Ensure recipients is a list
    if isinstance(recipients, str):
        recipients = [recipients]
    
    # Get SMTP settings
    smtp_config = config.get('smtp', {})
    smtp_host = smtp_config.get('host')
    smtp_port = smtp_config.get('port', 587)
    smtp_username = smtp_config.get('username')
    smtp_password = smtp_config.get('password')
    smtp_use_tls = smtp_config.get('use_tls', True)
    sender = smtp_config.get('sender', smtp_username)
    
    if not smtp_host or not smtp_username or not smtp_password:
        logger.warning("SMTP configuration incomplete, skipping notification")
        return False
    
    # Determine if this is an error notification
    has_errors = report.get('accounts_with_errors', 0) > 0 or report.get('total_errors', 0) > 0
    
    # Check if we should send based on send_on setting
    send_on = config.get('send_on', 'always')
    if send_on == 'error' and not has_errors:
        logger.debug("Notification skipped: send_on='error' but no errors occurred")
        return False
    
    # Build subject
    if has_errors:
        subject = f"⚠️ [PEC Archiver] Errori nel backup del {target_date.strftime('%Y-%m-%d')}"
    else:
        subject = f"✅ [PEC Archiver] Backup completato - {target_date.strftime('%Y-%m-%d')}"
    
    # Create message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    
    # Attach plain text and HTML versions
    text_content = format_report_text(report, target_date)
    html_content = format_report_html(report, target_date)
    
    msg.attach(MIMEText(text_content, 'plain', 'utf-8'))
    msg.attach(MIMEText(html_content, 'html', 'utf-8'))
    
    # Send email
    try:
        if smtp_use_tls:
            server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30)
        
        server.login(smtp_username, smtp_password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        
        logger.info(f"Notification sent successfully to {len(recipients)} recipient(s)")
        return True
    except smtplib.SMTPException as e:
        logger.error(f"Failed to send notification: {e}")
        raise NotificationError(f"SMTP error: {e}")
    except Exception as e:
        logger.error(f"Failed to send notification: {e}")
        raise NotificationError(f"Failed to send notification: {e}")


def validate_notification_config(config: dict) -> list[str]:
    """
    Validate notification configuration.
    
    Args:
        config: Notification configuration dictionary
    
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    if not isinstance(config, dict):
        return ["notifications must be a dictionary"]
    
    # If not enabled, no further validation needed
    if not config.get('enabled', False):
        return []
    
    # Check recipients
    recipients = config.get('recipients', [])
    if not recipients:
        errors.append("notifications.recipients must contain at least one email address")
    elif isinstance(recipients, str):
        # Single recipient as string is allowed
        pass
    elif not isinstance(recipients, list):
        errors.append("notifications.recipients must be a string or list of strings")
    
    # Check SMTP config
    smtp = config.get('smtp', {})
    if not isinstance(smtp, dict):
        errors.append("notifications.smtp must be a dictionary")
    else:
        if not smtp.get('host'):
            errors.append("notifications.smtp.host is required when notifications are enabled")
        if not smtp.get('username'):
            errors.append("notifications.smtp.username is required when notifications are enabled")
        if not smtp.get('password'):
            errors.append("notifications.smtp.password is required when notifications are enabled")
    
    # Check send_on value
    send_on = config.get('send_on', 'always')
    if send_on not in ['always', 'error']:
        errors.append("notifications.send_on must be 'always' or 'error'")
    
    return errors
