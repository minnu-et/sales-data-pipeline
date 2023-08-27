"""
Watermark Manager Module
========================
Manages watermark files for incremental data processing.

A watermark represents the last successfully processed timestamp for a dataset.
This module handles reading, writing, and updating watermarks stored in S3 or locally.

Usage:
    from src.main.utility.watermark_manager import WatermarkManager
    
    wm = WatermarkManager(config, spark, entity="sales")
    last_processed = wm.get_watermark()  # "2026-02-04 23:59:59"
    
    # ... process data ...
    
    wm.update_watermark("2026-02-05 23:59:59")
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession


class WatermarkManager:
    """
    Manages watermark tracking for incremental data processing.
    
    Attributes:
        config (Dict): Configuration dictionary from config_loader
        spark (SparkSession): Active Spark session for S3 operations
        entity (str): Name of the entity (e.g., "sales", "customer")
        logger (Logger): Logger instance
    """
    
    def __init__(self, config: Dict[str, Any], spark: SparkSession, entity: str):
        """
        Initialize WatermarkManager.
        
        Args:
            config: Configuration dictionary containing watermark settings
            spark: SparkSession for S3 file operations
            entity: Entity name (sales, product, store, customer)
        """
        self.config = config
        self.spark = spark
        self.entity = entity
        self.logger = logging.getLogger(__name__)
        
        # Extract watermark configuration
        self.watermark_config = config.get("watermark", {})
        self.storage_type = self.watermark_config.get("storage", {}).get("type", "local")
        self.base_path = self.watermark_config.get("storage", {}).get("base_path", "")
        self.local_backup = self.watermark_config.get("storage", {}).get("local_backup", "resources/watermarks")
        
        # Get entity-specific settings
        self.watermark_file = self.watermark_config.get("files", {}).get(entity, f"{entity}_watermark.json")
        self.timestamp_column = self.watermark_config.get("timestamp_columns", {}).get(entity, "created_date")
        self.default_watermark = self.watermark_config.get("default", {}).get(entity, "2020-01-01 00:00:00")
        
        # Behavior settings
        self.buffer_minutes = self.watermark_config.get("behavior", {}).get("buffer_minutes", 5)
        self.initial_lookback_days = self.watermark_config.get("behavior", {}).get("initial_load_lookback_days", 365)
        
        self.logger.info(f"WatermarkManager initialized for entity: {entity}")
        self.logger.info(f"Storage type: {self.storage_type}, File: {self.watermark_file}")
    
    def get_watermark(self) -> str:
        """
        Retrieve the last processed watermark for this entity.
        
        Returns:
            str: Timestamp string (e.g., "2026-02-04 23:59:59")
            
        Logic:
            1. Try to read from S3 (if storage_type is s3)
            2. Fall back to local backup
            3. If no watermark exists, return default or calculated initial value
        """
        try:
            # Determine full path based on storage type
            if self.storage_type == "s3":
                watermark_path = f"{self.base_path}/{self.watermark_file}"
                watermark_data = self._read_from_s3(watermark_path)
                
                if watermark_data:
                    self.logger.info(f"Watermark read from S3: {watermark_data['last_processed_timestamp']}")
                    return watermark_data["last_processed_timestamp"]
                else:
                    # Try local backup
                    self.logger.warning("S3 watermark not found, trying local backup")
                    watermark_data = self._read_from_local()
                    if watermark_data:
                        return watermark_data["last_processed_timestamp"]
            else:
                # Local storage
                watermark_data = self._read_from_local()
                if watermark_data:
                    self.logger.info(f"Watermark read from local: {watermark_data['last_processed_timestamp']}")
                    return watermark_data["last_processed_timestamp"]
            
            # No watermark found - this is the first run
            self.logger.warning(f"No watermark found for {self.entity}. Using initial watermark.")
            return self._get_initial_watermark()
            
        except Exception as e:
            self.logger.error(f"Error reading watermark: {str(e)}")
            self.logger.info(f"Falling back to default watermark: {self.default_watermark}")
            return self.default_watermark
    
    def update_watermark(self, new_timestamp: str, metadata: Optional[Dict] = None) -> bool:
        """
        Update the watermark with a new timestamp.
        
        Args:
            new_timestamp: New watermark value (e.g., "2026-02-05 23:59:59")
            metadata: Optional dict with additional metadata (records_processed, run_id, etc.)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create watermark data structure
            watermark_data = {
                "entity": self.entity,
                "last_processed_timestamp": new_timestamp,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp_column": self.timestamp_column,
                "metadata": metadata or {}
            }
            
            # Write to primary storage
            if self.storage_type == "s3":
                watermark_path = f"{self.base_path}/{self.watermark_file}"
                success = self._write_to_s3(watermark_path, watermark_data)
                
                # Also write to local backup
                self._write_to_local(watermark_data)
                
                if success:
                    self.logger.info(f"Watermark updated in S3: {new_timestamp}")
                    return True
            else:
                success = self._write_to_local(watermark_data)
                if success:
                    self.logger.info(f"Watermark updated locally: {new_timestamp}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error updating watermark: {str(e)}")
            return False
    
    def get_filter_condition(self) -> str:
        """
        Generate a Spark SQL filter condition for incremental processing.
        
        Returns:
            str: SQL WHERE clause (e.g., "sales_date > '2026-02-04 23:59:59'")
        """
        watermark = self.get_watermark()
        
        # Apply buffer for late-arriving data
        if self.buffer_minutes > 0:
            watermark_dt = datetime.strptime(watermark, "%Y-%m-%d %H:%M:%S")
            buffered_dt = watermark_dt - timedelta(minutes=self.buffer_minutes)
            watermark = buffered_dt.strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"Applied {self.buffer_minutes} min buffer: {watermark}")
        
        filter_condition = f"{self.timestamp_column} > '{watermark}'"
        self.logger.info(f"Filter condition: {filter_condition}")
        return filter_condition
    
    def _get_initial_watermark(self) -> str:
        """
        Calculate the initial watermark for first-time runs.
        
        Returns:
            str: Initial watermark timestamp
        """
        if self.initial_lookback_days > 0:
            # Calculate lookback date from today
            lookback_date = datetime.now() - timedelta(days=self.initial_lookback_days)
            initial_watermark = lookback_date.strftime("%Y-%m-%d 00:00:00")
            self.logger.info(f"Calculated initial watermark ({self.initial_lookback_days} days lookback): {initial_watermark}")
        else:
            initial_watermark = self.default_watermark
            self.logger.info(f"Using default watermark: {initial_watermark}")
        
        return initial_watermark
    
    def _read_from_s3(self, path: str) -> Optional[Dict]:
        """
        Read watermark JSON file from S3.
        
        Args:
            path: S3 path to watermark file
            
        Returns:
            Dict or None if file doesn't exist
        """
        try:
            # Use Spark to read from S3
            sc = self.spark.sparkContext
            
            # Check if file exists
            hadoop_conf = sc._jsc.hadoopConfiguration()
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                sc._jvm.java.net.URI(path.replace("s3a://", "s3://")),
                hadoop_conf
            )
            file_path = sc._jvm.org.apache.hadoop.fs.Path(path)
            
            if not fs.exists(file_path):
                self.logger.warning(f"S3 file does not exist: {path}")
                return None
            
            # Read the file
            rdd = sc.textFile(path)
            json_content = rdd.collect()
            
            if json_content:
                watermark_data = json.loads(json_content[0])
                return watermark_data
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error reading from S3: {str(e)}")
            return None
    
    def _write_to_s3(self, path: str, data: Dict) -> bool:
        """
        Write watermark JSON file to S3.
        
        Args:
            path: S3 path to write watermark file
            data: Watermark data dictionary
            
        Returns:
            bool: True if successful
        """
        try:
            json_content = json.dumps(data, indent=2)
            
            # Use Spark RDD to write to S3
            sc = self.spark.sparkContext
            rdd = sc.parallelize([json_content])
            rdd.saveAsTextFile(path + "_temp")
            
            # Move from temp location to final location (overwrite)
            hadoop_conf = sc._jsc.hadoopConfiguration()
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                sc._jvm.java.net.URI(path.replace("s3a://", "s3://")),
                hadoop_conf
            )
            
            temp_path = sc._jvm.org.apache.hadoop.fs.Path(path + "_temp/part-00000")
            final_path = sc._jvm.org.apache.hadoop.fs.Path(path)
            
            # Delete old file if exists
            if fs.exists(final_path):
                fs.delete(final_path, False)
            
            # Rename temp to final
            fs.rename(temp_path, final_path)
            
            # Clean up temp directory
            fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path + "_temp"), True)
            
            self.logger.info(f"Successfully wrote watermark to S3: {path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing to S3: {str(e)}")
            return False
    
    def _read_from_local(self) -> Optional[Dict]:
        """
        Read watermark JSON file from local filesystem.
        
        Returns:
            Dict or None if file doesn't exist
        """
        try:
            local_path = Path(self.local_backup) / self.watermark_file
            
            if not local_path.exists():
                self.logger.warning(f"Local watermark file does not exist: {local_path}")
                return None
            
            with open(local_path, 'r') as f:
                watermark_data = json.load(f)
            
            return watermark_data
            
        except Exception as e:
            self.logger.error(f"Error reading from local: {str(e)}")
            return None
    
    def _write_to_local(self, data: Dict) -> bool:
        """
        Write watermark JSON file to local filesystem.
        
        Args:
            data: Watermark data dictionary
            
        Returns:
            bool: True if successful
        """
        try:
            local_path = Path(self.local_backup)
            local_path.mkdir(parents=True, exist_ok=True)
            
            file_path = local_path / self.watermark_file
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logger.info(f"Successfully wrote watermark locally: {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing to local: {str(e)}")
            return False
