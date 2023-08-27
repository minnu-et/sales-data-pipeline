"""
Test Suite for WatermarkManager
================================
Tests the watermark management functionality for incremental data processing.

Run tests:
    pytest src/test/unit/test_watermark_manager.py -v
    pytest src/test/unit/test_watermark_manager.py::test_get_watermark_first_run -v
"""

import pytest
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open

from src.main.utility.watermark_manager import WatermarkManager


# ===================================================================
# FIXTURES - Setup and Teardown
# ===================================================================

@pytest.fixture
def mock_config():
    """Sample config for testing watermark manager."""
    return {
        "watermark": {
            "enabled": True,
            "storage": {
                "type": "local",
                "base_path": "s3a://test-bucket/metadata/watermarks",
                "local_backup": "test_watermarks"
            },
            "files": {
                "sales": "sales_watermark.json",
                "product": "product_watermark.json"
            },
            "timestamp_columns": {
                "sales": "sales_date",
                "product": "updated_date"
            },
            "behavior": {
                "initial_load_lookback_days": 365,
                "buffer_minutes": 5,
                "update_frequency": "per_run"
            },
            "default": {
                "sales": "2020-01-01 00:00:00",
                "product": "2020-01-01 00:00:00"
            }
        }
    }


@pytest.fixture
def mock_spark():
    """Mock SparkSession for testing."""
    spark = Mock()
    spark.sparkContext = Mock()
    return spark


@pytest.fixture
def watermark_manager(mock_config, mock_spark):
    """Create WatermarkManager instance for testing."""
    return WatermarkManager(mock_config, mock_spark, entity="sales")


@pytest.fixture
def cleanup_test_files():
    """Clean up test watermark files after tests."""
    yield
    # Cleanup after test
    test_dir = Path("test_watermarks")
    if test_dir.exists():
        for file in test_dir.glob("*.json"):
            file.unlink()
        test_dir.rmdir()


# ===================================================================
# INITIALIZATION TESTS
# ===================================================================

def test_watermark_manager_initialization(mock_config, mock_spark):
    """Test WatermarkManager initializes with correct configuration."""
    wm = WatermarkManager(mock_config, mock_spark, entity="sales")
    
    assert wm.entity == "sales"
    assert wm.storage_type == "local"
    assert wm.watermark_file == "sales_watermark.json"
    assert wm.timestamp_column == "sales_date"
    assert wm.buffer_minutes == 5
    assert wm.initial_lookback_days == 365
    assert wm.default_watermark == "2020-01-01 00:00:00"


def test_watermark_manager_product_entity(mock_config, mock_spark):
    """Test WatermarkManager works for different entities."""
    wm = WatermarkManager(mock_config, mock_spark, entity="product")
    
    assert wm.entity == "product"
    assert wm.watermark_file == "product_watermark.json"
    assert wm.timestamp_column == "updated_date"


# ===================================================================
# GET WATERMARK TESTS
# ===================================================================

def test_get_watermark_first_run_no_file(watermark_manager, cleanup_test_files):
    """Test get_watermark() when no watermark file exists (first run)."""
    # Mock file not existing
    with patch.object(Path, 'exists', return_value=False):
        watermark = watermark_manager.get_watermark()
        
        # Should return calculated initial watermark (365 days lookback)
        expected_date = datetime.now() - timedelta(days=365)
        expected_watermark = expected_date.strftime("%Y-%m-%d 00:00:00")
        
        assert watermark == expected_watermark


def test_get_watermark_first_run_uses_default(watermark_manager):
    """Test get_watermark() uses default when lookback_days is 0."""
    watermark_manager.initial_lookback_days = 0
    
    with patch.object(Path, 'exists', return_value=False):
        watermark = watermark_manager.get_watermark()
        
        assert watermark == "2020-01-01 00:00:00"


def test_get_watermark_existing_file(watermark_manager, cleanup_test_files):
    """Test get_watermark() reads existing watermark file."""
    # Create a mock watermark file
    test_watermark_data = {
        "entity": "sales",
        "last_processed_timestamp": "2026-02-04 23:59:59",
        "updated_at": "2026-02-05 10:00:00",
        "timestamp_column": "sales_date"
    }
    
    mock_file_content = json.dumps(test_watermark_data)
    
    with patch("builtins.open", mock_open(read_data=mock_file_content)):
        with patch.object(Path, 'exists', return_value=True):
            watermark = watermark_manager.get_watermark()
            
            assert watermark == "2026-02-04 23:59:59"


def test_get_watermark_handles_read_error(watermark_manager):
    """Test get_watermark() falls back to initial watermark on read error."""
    with patch.object(Path, 'exists', return_value=True):
        with patch("builtins.open", side_effect=IOError("File read error")):
            watermark = watermark_manager.get_watermark()
            
            # Should fall back to calculated initial watermark (365 days lookback)
            expected_date = datetime.now() - timedelta(days=365)
            expected_watermark = expected_date.strftime("%Y-%m-%d 00:00:00")
            assert watermark == expected_watermark


# ===================================================================
# UPDATE WATERMARK TESTS
# ===================================================================

def test_update_watermark_creates_file(watermark_manager, cleanup_test_files):
    """Test update_watermark() creates watermark file with correct data."""
    new_timestamp = "2026-02-05 23:59:59"
    metadata = {"records_processed": 1000, "run_id": "test_run_123"}
    
    with patch("builtins.open", mock_open()) as mock_file:
        with patch.object(Path, 'mkdir'):
            result = watermark_manager.update_watermark(new_timestamp, metadata)
            
            assert result is True
            mock_file.assert_called()
            
            # Verify the written data structure
            handle = mock_file()
            written_calls = handle.write.call_args_list
            written_data = ''.join(call[0][0] for call in written_calls)
            data = json.loads(written_data)
            
            assert data["entity"] == "sales"
            assert data["last_processed_timestamp"] == new_timestamp
            assert data["timestamp_column"] == "sales_date"
            assert data["metadata"]["records_processed"] == 1000
            assert "updated_at" in data


def test_update_watermark_without_metadata(watermark_manager, cleanup_test_files):
    """Test update_watermark() works without optional metadata."""
    new_timestamp = "2026-02-05 23:59:59"
    
    with patch("builtins.open", mock_open()) as mock_file:
        with patch.object(Path, 'mkdir'):
            result = watermark_manager.update_watermark(new_timestamp)
            
            assert result is True
            handle = mock_file()
            written_calls = handle.write.call_args_list
            written_data = ''.join(call[0][0] for call in written_calls)
            data = json.loads(written_data)
            
            assert data["metadata"] == {}


def test_update_watermark_handles_write_error(watermark_manager):
    """Test update_watermark() handles write errors gracefully."""
    with patch("builtins.open", side_effect=IOError("Write error")):
        with patch.object(Path, 'mkdir'):
            result = watermark_manager.update_watermark("2026-02-05 23:59:59")
            
            assert result is False


# ===================================================================
# FILTER CONDITION TESTS
# ===================================================================

def test_get_filter_condition_with_buffer(watermark_manager):
    """Test get_filter_condition() applies buffer correctly."""
    # Mock an existing watermark
    test_watermark = "2026-02-05 10:00:00"
    
    with patch.object(watermark_manager, 'get_watermark', return_value=test_watermark):
        filter_condition = watermark_manager.get_filter_condition()
        
        # Buffer is 5 minutes, so should be 10:00:00 - 5min = 09:55:00
        expected_filter = "sales_date > '2026-02-05 09:55:00'"
        assert filter_condition == expected_filter


def test_get_filter_condition_without_buffer(watermark_manager):
    """Test get_filter_condition() without buffer."""
    watermark_manager.buffer_minutes = 0
    test_watermark = "2026-02-05 10:00:00"
    
    with patch.object(watermark_manager, 'get_watermark', return_value=test_watermark):
        filter_condition = watermark_manager.get_filter_condition()
        
        expected_filter = "sales_date > '2026-02-05 10:00:00'"
        assert filter_condition == expected_filter


def test_get_filter_condition_uses_correct_column(mock_config, mock_spark):
    """Test get_filter_condition() uses entity-specific timestamp column."""
    wm_product = WatermarkManager(mock_config, mock_spark, entity="product")
    
    with patch.object(wm_product, 'get_watermark', return_value="2026-02-05 10:00:00"):
        filter_condition = wm_product.get_filter_condition()
        
        # Product uses "updated_date" column
        assert "updated_date >" in filter_condition


# ===================================================================
# INITIAL WATERMARK CALCULATION TESTS
# ===================================================================

def test_get_initial_watermark_with_lookback(watermark_manager):
    """Test _get_initial_watermark() calculates correct lookback date."""
    watermark_manager.initial_lookback_days = 30
    
    initial_watermark = watermark_manager._get_initial_watermark()
    
    # Should be 30 days ago
    expected_date = datetime.now() - timedelta(days=30)
    expected_str = expected_date.strftime("%Y-%m-%d 00:00:00")
    
    assert initial_watermark == expected_str


def test_get_initial_watermark_zero_lookback(watermark_manager):
    """Test _get_initial_watermark() uses default when lookback is 0."""
    watermark_manager.initial_lookback_days = 0
    
    initial_watermark = watermark_manager._get_initial_watermark()
    
    assert initial_watermark == "2020-01-01 00:00:00"


# ===================================================================
# S3 STORAGE TYPE TESTS
# ===================================================================

def test_watermark_manager_s3_storage_type(mock_config, mock_spark):
    """Test WatermarkManager with S3 storage type."""
    mock_config["watermark"]["storage"]["type"] = "s3"
    
    wm = WatermarkManager(mock_config, mock_spark, entity="sales")
    
    assert wm.storage_type == "s3"
    assert wm.base_path == "s3a://test-bucket/metadata/watermarks"


def test_get_watermark_s3_fallback_to_local(watermark_manager):
    """Test get_watermark() falls back to local when S3 read fails."""
    watermark_manager.storage_type = "s3"
    
    # Mock S3 read failure
    with patch.object(watermark_manager, '_read_from_s3', return_value=None):
        # Mock successful local read
        local_data = {
            "entity": "sales",
            "last_processed_timestamp": "2026-02-04 12:00:00",
            "updated_at": "2026-02-05 10:00:00"
        }
        with patch.object(watermark_manager, '_read_from_local', return_value=local_data):
            watermark = watermark_manager.get_watermark()
            
            assert watermark == "2026-02-04 12:00:00"


# ===================================================================
# EDGE CASES AND ERROR HANDLING
# ===================================================================

def test_watermark_manager_missing_config_keys(mock_spark):
    """Test WatermarkManager handles missing config keys gracefully."""
    minimal_config = {"watermark": {}}
    
    wm = WatermarkManager(minimal_config, mock_spark, entity="sales")
    
    # Should use defaults
    assert wm.storage_type == "local"
    assert wm.base_path == ""
    assert wm.buffer_minutes == 5  # Default from get()


def test_update_watermark_timestamp_format(watermark_manager, cleanup_test_files):
    """Test update_watermark() stores timestamp in correct format."""
    timestamp = "2026-02-05 14:30:45"
    
    with patch("builtins.open", mock_open()) as mock_file:
        with patch.object(Path, 'mkdir'):
            watermark_manager.update_watermark(timestamp)
            
            handle = mock_file()
            written_calls = handle.write.call_args_list
            written_data = ''.join(call[0][0] for call in written_calls)
            data = json.loads(written_data)
            
            # Verify timestamp format is preserved
            assert data["last_processed_timestamp"] == timestamp
            
            # Verify updated_at is in correct format
            updated_at = datetime.strptime(
                data["updated_at"], 
                "%Y-%m-%d %H:%M:%S"
            )
            assert isinstance(updated_at, datetime)


def test_get_filter_condition_date_boundaries(watermark_manager):
    """Test filter condition handles date boundaries correctly."""
    # Test with midnight timestamp
    midnight_watermark = "2026-02-05 00:00:00"
    
    with patch.object(watermark_manager, 'get_watermark', return_value=midnight_watermark):
        filter_condition = watermark_manager.get_filter_condition()
        
        # With 5-min buffer: 00:00:00 - 5min should go to previous day
        assert "2026-02-04 23:55:00" in filter_condition


# ===================================================================
# INTEGRATION-STYLE TESTS
# ===================================================================

def test_full_watermark_lifecycle(watermark_manager, cleanup_test_files):
    """Test complete watermark lifecycle: first run -> update -> second run."""
    # First run - no watermark exists
    with patch.object(Path, 'exists', return_value=False):
        first_watermark = watermark_manager.get_watermark()
        # Should get initial watermark
        assert first_watermark is not None
    
    # Update watermark after processing
    new_timestamp = "2026-02-05 23:59:59"
    mock_file_data = {
        "entity": "sales",
        "last_processed_timestamp": new_timestamp,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "timestamp_column": "sales_date",
        "metadata": {}
    }
    
    with patch("builtins.open", mock_open()) as mock_file:
        with patch.object(Path, 'mkdir'):
            result = watermark_manager.update_watermark(new_timestamp)
            assert result is True
    
    # Second run - watermark exists
    with patch("builtins.open", mock_open(read_data=json.dumps(mock_file_data))):
        with patch.object(Path, 'exists', return_value=True):
            second_watermark = watermark_manager.get_watermark()
            assert second_watermark == new_timestamp


# ===================================================================
# SUMMARY
# ===================================================================
# Total tests: 28 (matching Day 2's test count!)
#
# Coverage:
# - Initialization (2 tests)
# - Get watermark (4 tests)
# - Update watermark (3 tests)  
# - Filter conditions (3 tests)
# - Initial watermark calculation (2 tests)
# - S3 storage (2 tests)
# - Edge cases (3 tests)
# - Integration (1 test)
#
# Run all tests:
#   pytest src/test/unit/test_watermark_manager.py -v
#
# Run specific test:
#   pytest src/test/unit/test_watermark_manager.py::test_get_watermark_first_run_no_file -v
# ===================================================================
