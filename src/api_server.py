#!/usr/bin/env python3
"""
PEC Archiver API - Main Entry Point

REST API server for searching and downloading archived PEC emails.
"""

import sys
import os
import argparse
import logging

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uvicorn
from src.api import create_app, set_base_path


def setup_logging(level: str = 'INFO') -> None:
    """
    Configure logging for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='PEC Archiver API - REST API for email search and download',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start the API server
  python -m src.api_server
  
  # Start on custom port
  python -m src.api_server --port 8080
  
  # Enable debug mode
  python -m src.api_server --debug
  
  # Set custom archive path
  python -m src.api_server --base-path /custom/archive
        """
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='Host to bind the server (default: 0.0.0.0)'
    )
    
    parser.add_argument(
        '--port', '-p',
        type=int,
        default=8000,
        help='Port to bind the server (default: 8000)'
    )
    
    parser.add_argument(
        '--base-path', '-b',
        type=str,
        default=None,
        help='Base path for archive storage (default: uses PEC_ARCHIVE_BASE_PATH env var)'
    )
    
    parser.add_argument(
        '--log-level', '-l',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode with auto-reload'
    )
    
    return parser.parse_args()


def main() -> int:
    """
    Main entry point.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    args = parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # Set base path
    base_path = args.base_path or os.environ.get(
        'PEC_ARCHIVE_BASE_PATH',
        '/data/pec-archive'
    )
    set_base_path(base_path)
    
    logger.info(f"PEC Archiver API starting...")
    logger.info(f"Archive base path: {base_path}")
    logger.info(f"Server: http://{args.host}:{args.port}")
    logger.info(f"API Documentation: http://{args.host}:{args.port}/api/docs")
    
    try:
        uvicorn.run(
            "src.api:app",
            host=args.host,
            port=args.port,
            reload=args.debug,
            log_level=args.log_level.lower()
        )
        return 0
    except Exception as e:
        logger.error(f"Server error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
