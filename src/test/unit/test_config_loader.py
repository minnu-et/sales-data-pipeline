"""
Unit Tests for config_loader.py

These tests verify that the configuration loading system works correctly:
- Deep merge functionality
- Singleton pattern
- Environment variable overrides
- YAML file loading
- Error handling

Run with: pytest src/test/unit/test_config_loader.py -v
"""

import pytest
import os
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import the module we're testing
from main.utility.config_loader import (
    ConfigLoader,
    load_config,
    get_config_value,
    _config_instance
)


class TestDeepMerge:
    """Test the deep merge functionality."""
    
    def test_simple_merge(self):
        """Test merging two simple dictionaries."""
        loader = ConfigLoader.__new__(ConfigLoader)  # Create instance without __init__
        
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        
        result = loader._deep_merge(base, override)
        
        assert result == {"a": 1, "b": 3, "c": 4}
        assert result["b"] == 3  # Override value wins
    
    def test_nested_merge(self):
        """Test merging nested dictionaries."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        base = {
            "database": {
                "host": "localhost",
                "port": 5432
            }
        }
        override = {
            "database": {
                "port": 3306  # Only override port, keep host
            }
        }
        
        result = loader._deep_merge(base, override)
        
        assert result["database"]["host"] == "localhost"  # From base
        assert result["database"]["port"] == 3306  # From override
    
    def test_deep_nested_merge(self):
        """Test merging deeply nested structures."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        base = {
            "spark": {
                "config": {
                    "memory": "4g",
                    "cores": 4
                }
            }
        }
        override = {
            "spark": {
                "config": {
                    "memory": "8g"  # Only override memory
                }
            }
        }
        
        result = loader._deep_merge(base, override)
        
        assert result["spark"]["config"]["memory"] == "8g"
        assert result["spark"]["config"]["cores"] == 4
    
    def test_merge_with_non_dict_values(self):
        """Test that non-dict values are simply overridden."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        base = {"value": "old"}
        override = {"value": "new"}
        
        result = loader._deep_merge(base, override)
        
        assert result["value"] == "new"
    
    def test_merge_preserves_base_keys(self):
        """Test that keys only in base are preserved."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        base = {"a": 1, "b": 2, "c": 3}
        override = {"b": 20}
        
        result = loader._deep_merge(base, override)
        
        assert "a" in result
        assert "c" in result
        assert result["a"] == 1
        assert result["c"] == 3


class TestConfigLoaderInit:
    """Test ConfigLoader initialization."""
    
    @patch.object(ConfigLoader, '_load_env_file')
    @patch.object(ConfigLoader, '_validate_config_files')
    def test_init_with_explicit_env(self, mock_validate, mock_load_env):
        """Test initialization with explicit environment."""
        with patch.dict(os.environ, {}, clear=True):
            loader = ConfigLoader(env='prod')
            
            assert loader.env == 'prod'
            mock_load_env.assert_called_once()
            mock_validate.assert_called_once()
    
    @patch.object(ConfigLoader, '_load_env_file')
    @patch.object(ConfigLoader, '_validate_config_files')
    def test_init_defaults_to_dev(self, mock_validate, mock_load_env):
        """Test that default environment is 'dev'."""
        with patch.dict(os.environ, {}, clear=True):
            loader = ConfigLoader()
            
            assert loader.env == 'dev'
    
    @patch.object(ConfigLoader, '_load_env_file')
    @patch.object(ConfigLoader, '_validate_config_files')
    def test_init_reads_env_variable(self, mock_validate, mock_load_env):
        """Test that ENV environment variable is read."""
        with patch.dict(os.environ, {'ENV': 'staging'}):
            loader = ConfigLoader()
            
            assert loader.env == 'staging'


class TestConfigLoading:
    """Test actual config file loading."""
    
    def test_load_yaml_valid_file(self):
        """Test loading a valid YAML file."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        # Create temporary YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'test_key': 'test_value'}, f)
            temp_path = f.name
        
        try:
            result = loader._load_yaml(Path(temp_path))
            assert result == {'test_key': 'test_value'}
        finally:
            os.unlink(temp_path)
    
    def test_load_yaml_empty_file(self):
        """Test loading an empty YAML file."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('')  # Empty file
            temp_path = f.name
        
        try:
            result = loader._load_yaml(Path(temp_path))
            assert result == {}  # Should return empty dict
        finally:
            os.unlink(temp_path)
    
    def test_load_yaml_missing_file(self):
        """Test that loading missing file raises error."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        with pytest.raises(Exception):
            loader._load_yaml(Path('/nonexistent/file.yaml'))


class TestEnvironmentVariableSubstitution:
    """Test environment variable substitution in configs."""
    
    def test_substitute_env_var_simple(self):
        """Test substituting a simple environment variable."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        with patch.dict(os.environ, {'MY_BUCKET': 'test-bucket'}):
            config = {'bucket': '${MY_BUCKET}'}
            result = loader._substitute_env_vars(config)
            
            assert result['bucket'] == 'test-bucket'
    
    def test_substitute_env_var_nested(self):
        """Test substituting env vars in nested structures."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        with patch.dict(os.environ, {'DB_HOST': 'prod-db.example.com'}):
            config = {
                'database': {
                    'host': '${DB_HOST}',
                    'port': 5432
                }
            }
            result = loader._substitute_env_vars(config)
            
            assert result['database']['host'] == 'prod-db.example.com'
            assert result['database']['port'] == 5432
    
    def test_substitute_missing_env_var(self):
        """Test that missing env var is left as-is with warning."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        with patch.dict(os.environ, {}, clear=True):
            config = {'value': '${NONEXISTENT_VAR}'}
            result = loader._substitute_env_vars(config)
            
            # Should leave placeholder as-is when var not found
            assert result['value'] == '${NONEXISTENT_VAR}'
    
    def test_substitute_non_placeholder_values(self):
        """Test that non-placeholder values are unchanged."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        config = {
            'regular_string': 'hello',
            'number': 42,
            'list': [1, 2, 3]
        }
        result = loader._substitute_env_vars(config)
        
        assert result == config


class TestEnvironmentOverrides:
    """Test environment variable overrides."""
    
    def test_s3_bucket_override(self):
        """Test that S3_BUCKET_NAME env var overrides config."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        config = {'aws': {'s3': {'bucket_name': 'default-bucket'}}}
        
        with patch.dict(os.environ, {'S3_BUCKET_NAME': 'override-bucket'}):
            result = loader._apply_env_overrides(config)
            
            assert result['aws']['s3']['bucket_name'] == 'override-bucket'
    
    def test_log_level_override(self):
        """Test that LOG_LEVEL env var overrides config."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        config = {'logging': {'level': 'INFO'}}
        
        with patch.dict(os.environ, {'LOG_LEVEL': 'DEBUG'}):
            result = loader._apply_env_overrides(config)
            
            assert result['logging']['level'] == 'DEBUG'
    
    def test_no_override_when_env_var_missing(self):
        """Test that config is unchanged when env var not set."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        config = {'aws': {'s3': {'bucket_name': 'default-bucket'}}}
        
        with patch.dict(os.environ, {}, clear=True):
            result = loader._apply_env_overrides(config)
            
            assert result['aws']['s3']['bucket_name'] == 'default-bucket'


class TestSingletonPattern:
    """Test that singleton pattern works correctly."""
    
    def test_load_config_returns_same_instance(self):
        """Test that multiple calls return the same config instance."""
        # Reset singleton
        import main.utility.config_loader as config_module
        config_module._config_instance = None
        
        with patch.object(ConfigLoader, 'load') as mock_load:
            mock_load.return_value = {'test': 'config'}
            
            # First call
            config1 = load_config()
            
            # Second call
            config2 = load_config()
            
            # Should be the same instance
            assert config1 is config2
            
            # Load should only be called once
            assert mock_load.call_count == 1
    
    def test_force_reload_reloads_config(self):
        """Test that force_reload=True reloads the config."""
        import main.utility.config_loader as config_module
        config_module._config_instance = None
        
        with patch.object(ConfigLoader, 'load') as mock_load:
            mock_load.return_value = {'test': 'config'}
            
            # First call
            config1 = load_config()
            
            # Force reload
            config2 = load_config(force_reload=True)
            
            # Load should be called twice
            assert mock_load.call_count == 2


class TestGetConfigValue:
    """Test the get_config_value helper function."""
    
    def test_get_simple_value(self):
        """Test getting a simple top-level value."""
        import main.utility.config_loader as config_module
        config_module._config_instance = {'key': 'value'}
        
        result = get_config_value('key')
        assert result == 'value'
    
    def test_get_nested_value(self):
        """Test getting a nested value with dot notation."""
        import main.utility.config_loader as config_module
        config_module._config_instance = {
            'aws': {
                's3': {
                    'bucket_name': 'my-bucket'
                }
            }
        }
        
        result = get_config_value('aws.s3.bucket_name')
        assert result == 'my-bucket'
    
    def test_get_missing_value_returns_default(self):
        """Test that missing key returns default value."""
        import main.utility.config_loader as config_module
        config_module._config_instance = {'key': 'value'}
        
        result = get_config_value('nonexistent.key', default='default_value')
        assert result == 'default_value'
    
    def test_get_missing_value_returns_none(self):
        """Test that missing key without default returns None."""
        import main.utility.config_loader as config_module
        config_module._config_instance = {'key': 'value'}
        
        result = get_config_value('nonexistent.key')
        assert result is None


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    @patch.object(ConfigLoader, '_load_env_file')
    def test_missing_env_config_shows_warning(self, mock_load_env):
        """Test that missing environment config shows warning (not error)."""
        with patch.dict(os.environ, {'ENV': 'test'}):
        # Should create loader successfully even if env config missing
        # (will use base config only)
             loader = ConfigLoader()
             assert loader.env == 'test'
    
    def test_load_yaml_invalid_yaml_raises_error(self):
        """Test that invalid YAML raises error."""
        loader = ConfigLoader.__new__(ConfigLoader)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('invalid: yaml: content:')  # Invalid YAML
            temp_path = f.name
        
        try:
            with pytest.raises(Exception):
                loader._load_yaml(Path(temp_path))
        finally:
            os.unlink(temp_path)


# Integration test (tests the whole flow)
class TestIntegration:
    """Integration tests for complete config loading flow."""
    
    @pytest.mark.integration
    def test_full_config_load_flow(self):
        """
        Test complete config loading with real files.
        
        This test requires actual config files to exist.
        Skip if running in isolation.
        """
        # This will use your actual config files
        # Only runs if config files exist
        config_path = Path('config/config.yaml')
        
        if config_path.exists():
            config = load_config(force_reload=True)
            
            # Basic assertions
            assert config is not None
            assert isinstance(config, dict)
            
            # Check expected top-level keys
            assert 'spark' in config
            assert 'aws' in config
            assert 'paths' in config
            assert 'logging' in config
            
            print("✅ Integration test passed - config loaded successfully!")
        else:
            pytest.skip("Config files not found - skipping integration test")


# Pytest fixtures (reusable test components)
@pytest.fixture
def sample_base_config():
    """Fixture providing a sample base config."""
    return {
        'spark': {
            'app_name': 'Test App',
            'master': 'local[*]',
            'log_level': 'WARN'
        },
        'aws': {
            's3': {
                'bucket_name': 'test-bucket',
                'region': 'us-east-1'
            }
        }
    }


@pytest.fixture
def sample_env_config():
    """Fixture providing a sample environment-specific config."""
    return {
        'spark': {
            'log_level': 'DEBUG'  # Override only log_level
        },
        'aws': {
            's3': {
                'bucket_name': 'dev-bucket'  # Override bucket
            }
        }
    }


# Test using fixtures
def test_merge_with_fixtures(sample_base_config, sample_env_config):
    """Test merge using fixtures."""
    loader = ConfigLoader.__new__(ConfigLoader)
    
    result = loader._deep_merge(sample_base_config, sample_env_config)
    
    # Check overrides applied
    assert result['spark']['log_level'] == 'DEBUG'
    assert result['aws']['s3']['bucket_name'] == 'dev-bucket'
    
    # Check base values preserved
    assert result['spark']['app_name'] == 'Test App'
    assert result['spark']['master'] == 'local[*]'
    assert result['aws']['s3']['region'] == 'us-east-1'


if __name__ == '__main__':
    """
    Run tests directly with: python test_config_loader.py
    Or use pytest: pytest test_config_loader.py -v
    """
    pytest.main([__file__, '-v', '--tb=short'])
