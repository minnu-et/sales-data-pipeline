"""
Data Validator Module
=====================
Integrates Great Expectations validation into the data pipeline.

Usage:
    from src.main.utility.data_validator import DataValidator
    
    validator = DataValidator()
    result = validator.validate_dataframe(df, "sales_suite")
    
    if not result.success:
        print(f"Validation failed: {result.statistics}")
"""

import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import DataFrame


@dataclass
class ValidationResult:
    """Container for validation results."""
    success: bool
    suite_name: str
    statistics: Dict[str, Any]
    failed_expectations: list
    validation_results: Any  # Full GE validation results
    

class DataValidator:
    """
    Handles data quality validation using Great Expectations.
    
    Attributes:
        context: GE DataContext
        logger: Logger instance
    """
    
    def __init__(self):
        """Initialize DataValidator with GE context."""
        self.logger = logging.getLogger(__name__)
        try:
            self.context = gx.get_context(project_root_dir=".")
            self.logger.info("DataValidator initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize GE context: {e}")
            raise
    
    def validate_dataframe(
        self, 
        df: DataFrame, 
        suite_name: str,
        batch_id: str = "default_batch"
    ) -> ValidationResult:
        """
        Validate a Spark DataFrame against an expectation suite.
        
        Args:
            df: Spark DataFrame to validate
            suite_name: Name of the expectation suite to use
            batch_id: Identifier for this batch (for tracking)
            
        Returns:
            ValidationResult object with success status and details
        """
        try:
            self.logger.info(f"Starting validation with suite: {suite_name}")
            
            # Create runtime batch request
            batch_request = RuntimeBatchRequest(
                datasource_name="spark_datasource",
                data_connector_name="runtime_connector",
                data_asset_name=suite_name.replace("_suite", ""),
                runtime_parameters={"batch_data": df},
                batch_identifiers={"batch_id": batch_id}
            )
            
            # Get validator directly (no checkpoint needed)
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name
            )
            
            # Run validation
            validation_result = validator.validate()
            
            # Parse results
            success = validation_result["success"]
            statistics = validation_result["statistics"]
            
            # Get failed expectations
            failed_expectations = []
            if not success:
                for result in validation_result["results"]:
                    if not result["success"]:
                        failed_expectations.append({
                            "expectation_type": result["expectation_config"]["expectation_type"],
                            "kwargs": result["expectation_config"]["kwargs"],
                            "result": result.get("result", {})
                        })
            
            # Log summary
            self.logger.info(
                f"Validation complete: {statistics['successful_expectations']}/{statistics['evaluated_expectations']} passed"
            )
            
            if not success:
                self.logger.warning(
                    f"Validation failed with {len(failed_expectations)} failures"
                )
            
            return ValidationResult(
                success=success,
                suite_name=suite_name,
                statistics=statistics,
                failed_expectations=failed_expectations,
                validation_results=validation_result
            )
            
        except Exception as e:
            self.logger.error(f"Validation error: {str(e)}")
            raise
    
    def validate_with_action(
        self,
        df: DataFrame,
        suite_name: str,
        on_failure: str = "log",
        batch_id: str = "default_batch"
    ) -> ValidationResult:
        """
        Validate DataFrame and take action on failure.
        
        Args:
            df: Spark DataFrame to validate
            suite_name: Name of expectation suite
            on_failure: Action to take on failure ("log", "warn", "raise")
            batch_id: Batch identifier
            
        Returns:
            ValidationResult object
            
        Raises:
            Exception: If on_failure="raise" and validation fails
        """
        result = self.validate_dataframe(df, suite_name, batch_id)
        
        if not result.success:
            failure_msg = (
                f"Data quality validation failed for {suite_name}:\n"
                f"  Passed: {result.statistics['successful_expectations']}\n"
                f"  Failed: {result.statistics['unsuccessful_expectations']}\n"
                f"  Total: {result.statistics['evaluated_expectations']}\n"
            )
            
            if on_failure == "raise":
                raise Exception(failure_msg)
            elif on_failure == "warn":
                self.logger.warning(failure_msg)
            else:  # log
                self.logger.info(failure_msg)
        
        return result
    
    def get_failed_rows_count(self, result: ValidationResult) -> int:
        """
        Estimate number of rows that failed validation.
        
        Args:
            result: ValidationResult from validation
            
        Returns:
            Approximate count of failed rows
        """
        if result.success:
            return 0
        
        # Sum up unexpected counts from failed expectations
        failed_count = 0
        for failure in result.failed_expectations:
            if "unexpected_count" in failure.get("result", {}):
                failed_count += failure["result"]["unexpected_count"]
        
        return failed_count
    
    def print_validation_summary(self, result: ValidationResult):
        """
        Print a human-readable validation summary.
        
        Args:
            result: ValidationResult to summarize
        """
        print("\n" + "=" * 70)
        print(f"VALIDATION SUMMARY: {result.suite_name}")
        print("=" * 70)
        
        stats = result.statistics
        print(f"Status: {'✓ PASSED' if result.success else '✗ FAILED'}")
        print(f"Successful: {stats['successful_expectations']}")
        print(f"Failed: {stats['unsuccessful_expectations']}")
        print(f"Total: {stats['evaluated_expectations']}")
        print(f"Success %: {stats['success_percent']:.1f}%")
        
        if not result.success:
            print("\nFailed Expectations:")
            for i, failure in enumerate(result.failed_expectations, 1):
                print(f"\n  {i}. {failure['expectation_type']}")
                print(f"     Column: {failure['kwargs'].get('column', 'N/A')}")
                if "unexpected_count" in failure.get("result", {}):
                    print(f"     Failures: {failure['result']['unexpected_count']} rows")
        
        print("=" * 70 + "\n")


# Convenience functions for common use cases
def validate_bronze_data(df: DataFrame, entity: str) -> ValidationResult:
    """
    Validate bronze layer data.
    
    Args:
        df: Spark DataFrame (bronze data)
        entity: Entity name (sales, product, customer, store)
        
    Returns:
        ValidationResult
    """
    validator = DataValidator()
    suite_name = f"{entity}_suite"
    return validator.validate_with_action(
        df, 
        suite_name, 
        on_failure="warn",
        batch_id=f"bronze_{entity}"
    )


def validate_silver_data(df: DataFrame, entity: str, strict: bool = True) -> ValidationResult:
    """
    Validate silver layer data (stricter validation).
    
    Args:
        df: Spark DataFrame (silver data)
        entity: Entity name
        strict: If True, raise exception on failure
        
    Returns:
        ValidationResult
    """
    validator = DataValidator()
    suite_name = f"{entity}_suite"
    return validator.validate_with_action(
        df,
        suite_name,
        on_failure="raise" if strict else "warn",
        batch_id=f"silver_{entity}"
    )
