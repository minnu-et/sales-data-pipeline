"""
DEBUG VERSION OF config_loader.py

This version has TONS of print statements so you can WATCH the execution flow!

HOW TO USE:
1. Copy this file to: src/main/utility/debug_config_loader.py
2. In main.py, change import to:
   from src.main.utility.debug_config_loader import load_config
3. Run your main.py and WATCH what happens!

After understanding the flow, switch back to regular config_loader.py
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class ConfigLoader:
    """
    ConfigLoader with DEBUG output - shows execution flow!
    """
    
    def __init__(self, env: Optional[str] = None):
        print("\n" + "🔵"*35)
        print("🔵 ConfigLoader.__init__() STARTED")
        print("🔵" + "="*70)
        print(f"🔵 Received parameter: env = {env}")
        
        # =====================================================================
        # STEP 1: Load .env file
        # =====================================================================
        print("\n🔵 STEP 1/4: Loading .env file...")
        self._load_env_file()
        
        # =====================================================================
        # STEP 2: Determine environment
        # =====================================================================
        print("\n🔵 STEP 2/4: Determining environment...")
        self.env = env or os.getenv('ENV', 'dev')
        print(f"🔵   → self.env = '{self.env}'")
        if env:
            print(f"🔵   (Came from parameter)")
        elif os.getenv('ENV'):
            print(f"🔵   (Came from ENV environment variable)")
        else:
            print(f"🔵   (Using default 'dev')")
        
        # =====================================================================
        # STEP 3: Set up paths
        # =====================================================================
        print("\n🔵 STEP 3/4: Finding project root and setting up paths...")
        self.project_root = self._find_project_root()
        print(f"🔵   → project_root = {self.project_root}")
        
        self.config_dir = self.project_root / 'config'
        print(f"🔵   → config_dir = {self.config_dir}")
        
        self.base_config_path = self.config_dir / 'config.yaml'
        print(f"🔵   → base_config_path = {self.base_config_path.name}")
        
        self.env_config_path = self.config_dir / 'environments' / f'{self.env}.yaml'
        print(f"🔵   → env_config_path = environments/{self.env}.yaml")
        
        # =====================================================================
        # STEP 4: Validate files exist
        # =====================================================================
        print("\n🔵 STEP 4/4: Validating config files exist...")
        self._validate_config_files()
        
        print("\n🔵 ✓ ConfigLoader.__init__() COMPLETED")
        print("🔵"*35 + "\n")
    
    def _load_env_file(self):
        print("  🟢 _load_env_file() called")
        
        env_path = Path.cwd() / '.env'
        print(f"  🟢   Checking current directory: {env_path}")
        
        if not env_path.exists():
            env_path = Path.cwd().parent / '.env'
            print(f"  🟢   Not found, trying parent: {env_path}")
        
        if env_path.exists():
            print(f"  🟢   ✓ Found .env file!")
            load_dotenv(env_path)
            print(f"  🟢   ✓ Loaded environment variables from .env")
            
            # Show what we loaded
            if os.getenv('ENV'):
                print(f"  🟢   ENV = {os.getenv('ENV')}")
            if os.getenv('S3_BUCKET_NAME'):
                print(f"  🟢   S3_BUCKET_NAME = {os.getenv('S3_BUCKET_NAME')}")
        else:
            print("  🟢   ⚠ .env file not found (will use system env vars)")
    
    def _find_project_root(self) -> Path:
        print("  🟢 _find_project_root() called")
        current = Path.cwd()
        print(f"  🟢   Starting from current directory: {current}")
        
        for i in range(3):
            check_path = current / 'config'
            print(f"  🟢   Iteration {i+1}: Looking for {check_path}")
            
            if check_path.exists():
                print(f"  🟢   ✓ Found config/ directory!")
                print(f"  🟢   ✓ Project root is: {current}")
                return current
            
            print(f"  🟢   Not found, going up one level...")
            current = current.parent
        
        print(f"  🟢   Using current directory as fallback: {Path.cwd()}")
        return Path.cwd()
    
    def _validate_config_files(self):
        print("  🟢 _validate_config_files() called")
        
        # Check base config
        print(f"  🟢   Checking base config: {self.base_config_path.name}")
        if not self.base_config_path.exists():
            print(f"  🟢   ✗ BASE CONFIG NOT FOUND!")
            raise FileNotFoundError(
                f"Base config not found: {self.base_config_path}\n"
                f"Expected at: {self.config_dir}/config.yaml"
            )
        else:
            print(f"  🟢   ✓ Base config exists")
        
        # Check environment config
        print(f"  🟢   Checking env config: environments/{self.env}.yaml")
        if not self.env_config_path.exists():
            print(f"  🟢   ⚠ Environment config NOT found (will use base only)")
        else:
            print(f"  🟢   ✓ Environment config exists")
    
    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        print(f"  🟡 _load_yaml() called")
        print(f"  🟡   Loading file: {path.name}")
        
        try:
            with open(path, 'r') as f:
                config = yaml.safe_load(f)
                
                if config:
                    num_keys = len(config)
                    print(f"  🟡   ✓ Loaded YAML successfully!")
                    print(f"  🟡   Top-level keys ({num_keys}): {list(config.keys())}")
                    return config
                else:
                    print(f"  🟡   ⚠ File is empty, returning empty dict")
                    return {}
        except Exception as e:
            print(f"  🟡   ✗ ERROR loading YAML: {str(e)}")
            raise Exception(f"Error loading {path}: {str(e)}")
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        print(f"  🟣 _deep_merge() called")
        print(f"  🟣   Base config has {len(base)} keys: {list(base.keys())}")
        print(f"  🟣   Override config has {len(override)} keys: {list(override.keys())}")
        
        result = base.copy()
        print(f"  🟣   Starting with copy of base config")
        
        for key, value in override.items():
            print(f"\n  🟣   Processing override key: '{key}'")
            
            # Check if we should do recursive merge
            key_exists = key in result
            base_is_dict = isinstance(result.get(key), dict)
            override_is_dict = isinstance(value, dict)
            
            print(f"  🟣     - Key exists in base? {key_exists}")
            print(f"  🟣     - Base value is dict? {base_is_dict}")
            print(f"  🟣     - Override value is dict? {override_is_dict}")
            
            if key_exists and base_is_dict and override_is_dict:
                print(f"  🟣     → ALL TRUE: Recursively merging nested dicts...")
                result[key] = self._deep_merge(result[key], value)
                print(f"  🟣     → Recursive merge complete for '{key}'")
            else:
                print(f"  🟣     → AT LEAST ONE FALSE: Simple override")
                if not key_exists:
                    print(f"  🟣       (New key being added)")
                print(f"  🟣       result['{key}'] = {value}")
                result[key] = value
        
        print(f"\n  🟣   Deep merge complete!")
        print(f"  🟣   Merged config has {len(result)} keys")
        return result
    
    def _substitute_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        print("  🟠 _substitute_env_vars() called")
        print("  🟠   Looking for ${VAR_NAME} placeholders...")
        
        substitutions_made = 0
        
        def substitute_value(value, path="root"):
            nonlocal substitutions_made
            
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                # Found a placeholder!
                var_name = value[2:-1]  # Extract variable name
                env_value = os.getenv(var_name)
                
                if env_value is not None:
                    print(f"  🟠   ✓ Substituting at {path}:")
                    print(f"  🟠     ${{{var_name}}} → '{env_value}'")
                    substitutions_made += 1
                    return env_value
                else:
                    print(f"  🟠   ⚠ Variable not found at {path}: ${{{var_name}}}")
                    return value
                    
            elif isinstance(value, dict):
                return {k: substitute_value(v, f"{path}.{k}") for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute_value(item, f"{path}[{i}]") for i, item in enumerate(value)]
            else:
                return value
        
        result = substitute_value(config)
        
        if substitutions_made > 0:
            print(f"  🟠   ✓ Made {substitutions_made} substitution(s)")
        else:
            print(f"  🟠   No ${} placeholders found")
        
        return result
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        print("  🔴 _apply_env_overrides() called")
        print("  🔴   Checking for direct environment variable overrides...")
        
        overrides_applied = 0
        
        # AWS S3 Bucket override
        if os.getenv('S3_BUCKET_NAME'):
            old_value = config.get('aws', {}).get('s3', {}).get('bucket_name', 'N/A')
            new_value = os.getenv('S3_BUCKET_NAME')
            config['aws']['s3']['bucket_name'] = new_value
            print(f"  🔴   ✓ Overriding S3 bucket:")
            print(f"  🔴     '{old_value}' → '{new_value}'")
            overrides_applied += 1
        
        # AWS Region override
        if os.getenv('AWS_DEFAULT_REGION'):
            old_value = config.get('aws', {}).get('s3', {}).get('region', 'N/A')
            new_value = os.getenv('AWS_DEFAULT_REGION')
            config['aws']['s3']['region'] = new_value
            print(f"  🔴   ✓ Overriding AWS region:")
            print(f"  🔴     '{old_value}' → '{new_value}'")
            overrides_applied += 1
        
        # Log level override
        if os.getenv('LOG_LEVEL'):
            old_value = config.get('logging', {}).get('level', 'N/A')
            new_value = os.getenv('LOG_LEVEL')
            config['logging']['level'] = new_value
            print(f"  🔴   ✓ Overriding log level:")
            print(f"  🔴     '{old_value}' → '{new_value}'")
            overrides_applied += 1
        
        # Pipeline mode override
        if os.getenv('PIPELINE_MODE'):
            old_value = config.get('pipeline', {}).get('mode', 'N/A')
            new_value = os.getenv('PIPELINE_MODE')
            config['pipeline']['mode'] = new_value
            print(f"  🔴   ✓ Overriding pipeline mode:")
            print(f"  🔴     '{old_value}' → '{new_value}'")
            overrides_applied += 1
        
        if overrides_applied == 0:
            print(f"  🔴   No environment variable overrides found")
        else:
            print(f"  🔴   Applied {overrides_applied} override(s)")
        
        return config
    
    def load(self) -> Dict[str, Any]:
        print("\n" + "🟦"*35)
        print("🟦 ConfigLoader.load() STARTED")
        print("🟦" + "="*70)
        
        # =====================================================================
        # STEP 1: Load base config
        # =====================================================================
        print("\n🟦 STEP 1/5: Loading base config...")
        base_config = self._load_yaml(self.base_config_path)
        
        # =====================================================================
        # STEP 2: Load environment-specific config
        # =====================================================================
        print("\n🟦 STEP 2/5: Loading environment-specific config...")
        if self.env_config_path.exists():
            env_config = self._load_yaml(self.env_config_path)
            
            # STEP 3: Deep merge
            print("\n🟦 STEP 3/5: Deep merging base + environment configs...")
            merged_config = self._deep_merge(base_config, env_config)
        else:
            print("🟦   No environment config found, using base config only")
            merged_config = base_config
        
        # =====================================================================
        # STEP 4: Substitute ${VAR} placeholders
        # =====================================================================
        print("\n🟦 STEP 4/5: Substituting environment variable placeholders...")
        final_config = self._substitute_env_vars(merged_config)
        
        # =====================================================================
        # STEP 5: Apply direct env var overrides
        # =====================================================================
        print("\n🟦 STEP 5/5: Applying direct environment variable overrides...")
        final_config = self._apply_env_overrides(final_config)
        
        print("\n🟦 ✓ ConfigLoader.load() COMPLETED")
        print("🟦 Final config ready!")
        print("🟦"*35 + "\n")
        
        return final_config


# =============================================================================
# SINGLETON PATTERN - Global variable to cache config
# =============================================================================
_config_instance = None


def load_config(env: Optional[str] = None, force_reload: bool = False) -> Dict[str, Any]:
    """
    Load configuration (singleton pattern with DEBUG output).
    """
    print("\n" + "="*70)
    print("⭐ load_config() FUNCTION CALLED")
    print("="*70)
    print(f"⭐ Parameters:")
    print(f"⭐   env = {env}")
    print(f"⭐   force_reload = {force_reload}")
    
    global _config_instance
    
    print(f"\n⭐ Checking singleton state...")
    if _config_instance is None:
        print(f"⭐   _config_instance is None (first time loading)")
    else:
        print(f"⭐   _config_instance exists (already loaded)")
    
    # =========================================================================
    # DECISION: Should we load config?
    # =========================================================================
    if _config_instance is None or force_reload:
        if _config_instance is None:
            print("\n⭐ DECISION: Loading config (first time)")
        else:
            print("\n⭐ DECISION: Reloading config (force_reload=True)")
        
        print("\n⭐ Creating ConfigLoader instance...")
        loader = ConfigLoader(env)
        
        print("\n⭐ Calling loader.load() to get final config...")
        _config_instance = loader.load()
        
        print("\n⭐ Config loaded and saved in _config_instance (singleton)")
        print(f"⭐ Config has {len(_config_instance)} top-level keys: {list(_config_instance.keys())}")
    else:
        print("\n⭐ DECISION: Returning CACHED config (singleton already exists)")
        print("⭐ No file reading needed - instant return!")
    
    print("\n⭐ load_config() RETURNING config dictionary")
    print("="*70 + "\n")
    
    return _config_instance


def get_config_value(key_path: str, default: Any = None) -> Any:
    """
    Get a config value using dot notation (with DEBUG output).
    """
    print(f"📍 get_config_value() called with key_path='{key_path}'")
    config = load_config()
    
    keys = key_path.split('.')
    print(f"📍   Navigating path: {' → '.join(keys)}")
    
    value = config
    
    try:
        for i, key in enumerate(keys):
            value = value[key]
            print(f"📍   Step {i+1}: value = value['{key}']")
        
        print(f"📍   ✓ Found value: {value}")
        return value
    except (KeyError, TypeError) as e:
        print(f"📍   ✗ Key not found, returning default: {default}")
        return default


def print_config_summary():
    """
    Print a summary of loaded configuration.
    """
    config = load_config()
    
    print("\n" + "="*70)
    print("CONFIGURATION SUMMARY")
    print("="*70)
    
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
    
    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    """
    Test the debug config loader.
    """
    print("\n" + "#"*70)
    print("# TESTING DEBUG CONFIG LOADER")
    print("#"*70 + "\n")
    
    # Load config
    config = load_config()
    
    # Print summary
    print_config_summary()
    
    # Test specific value access
    print("\nTesting get_config_value():")
    bucket = get_config_value('aws.s3.bucket_name')
    print(f"\nBucket name: {bucket}")
    
    print("\n" + "#"*70)
    print("# TEST COMPLETE")
    print("#"*70 + "\n")
