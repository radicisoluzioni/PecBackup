"""
Tests for backup_range module.
"""

import os
import sys
import tempfile
import pytest
import yaml
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from src.backup_range import (
    parse_date,
    validate_date_range,
    generate_date_range,
    validate_args,
)


class TestParseDate:
    """Tests for date parsing."""
    
    def test_parse_valid_date(self):
        """Test parsing a valid date string."""
        result = parse_date('2024-01-15')
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
    
    def test_parse_invalid_format(self):
        """Test parsing an invalid date format raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_date('15-01-2024')
        assert 'Invalid date format' in str(exc_info.value)
    
    def test_parse_invalid_date(self):
        """Test parsing an invalid date raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_date('2024-13-45')
        assert 'Invalid date format' in str(exc_info.value)


class TestValidateDateRange:
    """Tests for date range validation."""
    
    def test_valid_range(self):
        """Test a valid date range."""
        date_from = datetime(2024, 1, 15)
        date_to = datetime(2024, 1, 22)
        # Should not raise
        validate_date_range(date_from, date_to)
    
    def test_same_date_range(self):
        """Test a range with same start and end date."""
        date = datetime(2024, 1, 15)
        # Should not raise
        validate_date_range(date, date)
    
    def test_inverted_range(self):
        """Test that inverted range raises ValueError."""
        date_from = datetime(2024, 1, 22)
        date_to = datetime(2024, 1, 15)
        with pytest.raises(ValueError) as exc_info:
            validate_date_range(date_from, date_to)
        assert 'must be before or equal to' in str(exc_info.value)
    
    def test_future_date(self):
        """Test that future dates raise ValueError."""
        date_from = datetime(2024, 1, 15)
        date_to = datetime.now() + timedelta(days=10)
        with pytest.raises(ValueError) as exc_info:
            validate_date_range(date_from, date_to)
        assert 'cannot be in the future' in str(exc_info.value)


class TestGenerateDateRange:
    """Tests for date range generation."""
    
    def test_single_day_range(self):
        """Test generating a single day range."""
        date = datetime(2024, 1, 15)
        result = generate_date_range(date, date)
        assert len(result) == 1
        assert result[0] == date
    
    def test_week_range(self):
        """Test generating a week-long range."""
        date_from = datetime(2024, 1, 15)
        date_to = datetime(2024, 1, 21)
        result = generate_date_range(date_from, date_to)
        assert len(result) == 7
        assert result[0] == date_from
        assert result[-1] == date_to
    
    def test_month_range(self):
        """Test generating a month-long range."""
        date_from = datetime(2024, 1, 1)
        date_to = datetime(2024, 1, 31)
        result = generate_date_range(date_from, date_to)
        assert len(result) == 31


class TestValidateArgs:
    """Tests for argument validation."""
    
    def test_single_date_args(self):
        """Test validation with single date."""
        args = MagicMock()
        args.date = '2024-01-15'
        args.date_from = None
        args.date_to = None
        
        date_from, date_to = validate_args(args)
        assert date_from == date_to
        assert date_from.year == 2024
        assert date_from.month == 1
        assert date_from.day == 15
    
    def test_date_range_args(self):
        """Test validation with date range."""
        args = MagicMock()
        args.date = None
        args.date_from = '2024-01-15'
        args.date_to = '2024-01-22'
        
        date_from, date_to = validate_args(args)
        assert date_from.day == 15
        assert date_to.day == 22
    
    def test_no_dates_provided(self):
        """Test that no dates raises ValueError."""
        args = MagicMock()
        args.date = None
        args.date_from = None
        args.date_to = None
        
        with pytest.raises(ValueError) as exc_info:
            validate_args(args)
        assert 'must specify' in str(exc_info.value)
    
    def test_conflicting_args(self):
        """Test that conflicting date options raise ValueError."""
        args = MagicMock()
        args.date = '2024-01-15'
        args.date_from = '2024-01-10'
        args.date_to = None
        
        with pytest.raises(ValueError) as exc_info:
            validate_args(args)
        assert 'Cannot use --date together with' in str(exc_info.value)
    
    def test_date_from_without_date_to(self):
        """Test that date_from without date_to raises ValueError."""
        args = MagicMock()
        args.date = None
        args.date_from = '2024-01-15'
        args.date_to = None
        
        with pytest.raises(ValueError) as exc_info:
            validate_args(args)
        assert '--date-from requires --date-to' in str(exc_info.value)
    
    def test_date_to_without_date_from(self):
        """Test that date_to without date_from raises ValueError."""
        args = MagicMock()
        args.date = None
        args.date_from = None
        args.date_to = '2024-01-22'
        
        with pytest.raises(ValueError) as exc_info:
            validate_args(args)
        assert '--date-to requires --date-from' in str(exc_info.value)
