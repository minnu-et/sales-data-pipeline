"""
Configuration Loader Module

This module handles loading and merging configuration from multiple sources:
1. Base config (config/config.yaml)
2. Environment-specific overrides (config/environments/{env}.yaml)
3. Environment variables from .env file

Usage:
    from src.main.utility.config_loader import load_config
    
    config = load_config()  # Loads based on ENV variable
    # OR
    config = load_config('dev')  # Explicitly load dev config
    
    # Access config values:
    bucket = config['aws']['s3']['bucket_name']
    bronze_path = config['paths']['bronze']['sales']['raw']
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class ConfigLoader:
    """
    Handles loading and merging configuration files.
    """
    
    def __init__(self, env: Optional[str] = None):
        """
        Initialize ConfigLoader.
        
        Args:
            env: Environment name (dev, prod, etc.). 
                 If None, reads from ENV environment variable.
        """
        # Load .env file first (this sets environment variables)
        self._load_env_file()
        
        # Determine environment
        self.env = env or os.getenv('ENV', 'dev')
        
        # Set up paths
        self.project_root = self._find_project_root()
        self.config_dir = self.project_root / 'config'
        self.base_config_path = self.config_dir / 'config.yaml'
        self.env_config_path = self.config_dir / 'environments' / f'{self.env}.yaml'
        
        # Validate paths exist
        self._validate_config_files()
        
    def _load_env_file(self):
        """Load environment variables from .env file."""
        # Look for .env in current directory and parent directories
        env_path = Path.cwd() / '.env'
        if not env_path.exists():
            # Try one level up
            env_path = Path.cwd().parent / '.env'
        
        if env_path.exists():
            load_dotenv(env_path)
            print(f"✓ Loaded .env from: {env_path}")
        else:
            print("⚠ Warning: .env file not found. Using environment variables only.")
    
    def _find_project_root(self) -> Path:
        """
        Find project root directory (where config/ folder exists).
        
        Returns:
            Path to project root
        """
        current = Path.cwd()
        
        # Search up to 3 levels up
        for _ in range(3):
            if (current / 'config').exists():
                return current
            current = current.parent
        
        # If not found, assume current directory
        return Path.cwd()
    
    def _validate_config_files(self):
        """Validate that required config files exist."""
        if not self.base_config_path.exists():
            raise FileNotFoundError(
                f"Base config not found: {self.base_config_path}\n"
                f"Expected at: {self.config_dir}/config.yaml"
            )
        
        if not self.env_config_path.exists():
            print(f"⚠ Warning: Environment config not found: {self.env_config_path}")
            print(f"  Using base config only for environment: {self.env}")
    
    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        """
        Load YAML file.
        
        Args:
            path: Path to YAML file
            
        Returns:
            Dictionary with config data
        """
        try:
            with open(path, 'r') as f:
                config = yaml.safe_load(f)
                return config if config else {}
        except Exception as e:
            raise Exception(f"Error loading {path}: {str(e)}")
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.
        Override values take precedence over base values.
        
        Args:
            base: Base dictionary
            override: Override dictionary
            
        Returns:
            Merged dictionary
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Recursively merge nested dictionaries
                result[key] = self._deep_merge(result[key], value)
            else:
                # Override value
                result[key] = value
        
        return result
    
    def _substitute_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replace ${VAR_NAME} placeholders with environment variable values.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Configuration with substituted values
        """
        def substitute_value(value):
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                var_name = value[2:-1]
                env_value = os.getenv(var_name)
                if env_value is None:
                    print(f"⚠ Warning: Environment variable {var_name} not found")
                    return value
                return env_value
            elif isinstance(value, dict):
                return {k: substitute_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute_value(item) for item in value]
            else:
                return value
        
        return substitute_value(config)
    
    def load(self) -> Dict[str, Any]:
        """
        Load and merge all configuration sources.
        
        Returns:
            Complete merged configuration dictionary
        """
        print(f"\n{'='*60}")
        print(f"Loading configuration for environment: {self.env}")
        print(f"{'='*60}")
        
        # Load base config
        print(f"✓ Loading base config: {self.base_config_path.name}")
        base_config = self._load_yaml(self.base_config_path)
        
        # Load environment-specific config
        if self.env_config_path.exists():
            print(f"✓ Loading {self.env} config: {self.env_config_path.name}")
            env_config = self._load_yaml(self.env_config_path)
            
            # Merge configs (env overrides base)
            merged_config = self._deep_merge(base_config, env_config)
        else:
            merged_config = base_config
        
        # Substitute environment variables
        final_config = self._substitute_env_vars(merged_config)
        
        # Override with environment variables if they exist
        final_config = self._apply_env_overrides(final_config)
        
        print(f"✓ Configuration loaded successfully")
        print(f"{'='*60}\n")
        
        return final_config
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment variable overrides for common settings.
        
        Environment variables take highest precedence.
        
        Args:
            config: Current configuration
            
        Returns:
            Configuration with env var overrides applied
        """
        # AWS overrides
        if os.getenv('S3_BUCKET_NAME'):
            config['aws']['s3']['bucket_name'] = os.getenv('S3_BUCKET_NAME')
            print(f"  → Overriding S3 bucket from env: {os.getenv('S3_BUCKET_NAME')}")
        
        if os.getenv('AWS_DEFAULT_REGION'):
            config['aws']['s3']['region'] = os.getenv('AWS_DEFAULT_REGION')
            print(f"  → Overriding AWS region from env: {os.getenv('AWS_DEFAULT_REGION')}")
        
        # Logging overrides
        if os.getenv('LOG_LEVEL'):
            config['logging']['level'] = os.getenv('LOG_LEVEL')
            print(f"  → Overriding log level from env: {os.getenv('LOG_LEVEL')}")
        
        # Pipeline mode override
        if os.getenv('PIPELINE_MODE'):
            config['pipeline']['mode'] = os.getenv('PIPELINE_MODE')
            print(f"  → Overriding pipeline mode from env: {os.getenv('PIPELINE_MODE')}")
        
        return config


# Singleton instance for easy import
_config_instance = None


def load_config(env: Optional[str] = None, force_reload: bool = False) -> Dict[str, Any]:
    """
    Load configuration (singleton pattern).
    
    Args:
        env: Environment name (dev, prod, etc.). If None, uses ENV variable.
        force_reload: If True, reload config even if already loaded.
        
    Returns:
        Configuration dictionary
        
    Example:
        config = load_config('dev')
        bucket = config['aws']['s3']['bucket_name']
    """
    global _config_instance
    
    if _config_instance is None or force_reload:
        loader = ConfigLoader(env)
        _config_instance = loader.load()
    
    return _config_instance


def get_config_value(key_path: str, default: Any = None) -> Any:
    """
    Get a config value using dot notation.
    
    Args:
        key_path: Path to config value (e.g., 'aws.s3.bucket_name')
        default: Default value if key not found
        
    Returns:
        Config value or default
        
    Example:
        bucket = get_config_value('aws.s3.bucket_name')
        log_level = get_config_value('logging.level', 'INFO')
    """
    config = load_config()
    
    keys = key_path.split('.')
    value = config
    
    try:
        for key in keys:
            value = value[key]
        return value
    except (KeyError, TypeError):
        return default


def print_config_summary():
    """
    Print a summary of loaded configuration (useful for debugging).
    """
    config = load_config()
    
    print("\n" + "="*60)
    print("CONFIGURATION SUMMARY")
    print("="*60)
    
    print(f"\nEnvironment: {config['metadata']['environment']}")
    print(f"Project: {config['metadata']['project_name']}")
    print(f"Version: {config['metadata']['version']}")
    
    print(f"\nAWS Configuration:")
    print(f"  S3 Bucket: {config['aws']['s3']['bucket_name']}")
    print(f"  Region: {config['aws']['s3']['region']}")
    
    print(f"\nPipeline Settings:")
    print(f"  Mode: {config['pipeline']['mode']}")
    print(f"  Write Mode (Bronze): {config['pipeline']['write_modes']['bronze']}")
    print(f"  Write Mode (Silver): {config['pipeline']['write_modes']['silver']}")
    print(f"  Write Mode (Gold): {config['pipeline']['write_modes']['gold']}")
    
    print(f"\nLogging:")
    print(f"  Level: {config['logging']['level']}")
    print(f"  File: {config['logging']['file']['path']}")
    
    print(f"\nSpark:")
    print(f"  App Name: {config['spark']['app_name']}")
    print(f"  Master: {config['spark']['master']}")
    print(f"  Log Level: {config['spark']['log_level']}")
    
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    """
    Test the config loader.
    
    Run with: python -m src.main.utility.config_loader
    """
    print("Testing ConfigLoader...")
    
    # Load config
    config = load_config()
    
    # Print summary
    print_config_summary()
    
    # Test specific value access
    print("Testing get_config_value():")
    print(f"  Bucket name: {get_config_value('aws.s3.bucket_name')}")
    print(f"  Bronze sales path: {get_config_value('paths.bronze.sales.raw')}")
    print(f"  Log level: {get_config_value('logging.level')}")
    print(f"  Non-existent key: {get_config_value('fake.key', 'DEFAULT_VALUE')}")
    
    print("\n✓ ConfigLoader test complete!")
