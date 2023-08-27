"""
Great Expectations Setup Script
================================
Configures GE for PySpark DataFrames and creates expectation suites.

Run: python setup_great_expectations.py
"""

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest


def setup_ge_context():
    """Initialize and configure GE context."""
    print("Initializing Great Expectations context...")
    context = gx.get_context(project_root_dir=".")
    
    # Add Spark datasource for runtime dataframes
    datasource_config = {
        "name": "spark_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine"
        },
        "data_connectors": {
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"]
            }
        }
    }
    
    try:
        context.add_datasource(**datasource_config)
        print("✓ Spark datasource configured")
    except Exception as e:
        print(f"Datasource already exists or error: {e}")
    
    return context


def create_sales_expectations(context):
    """Create expectation suite for sales data."""
    print("\nCreating sales expectations...")
    
    suite_name = "sales_suite"
    
    # Create or get existing suite
    try:
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    except:
        suite = context.get_expectation_suite(expectation_suite_name=suite_name)
    
    # Import expectation classes
    from great_expectations.core import ExpectationConfiguration
    
    # Add expectations using proper API
    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_set",
            kwargs={
                "column_set": [
                    "customer_id", "store_id", "product_name", "sales_date",
                    "sales_person_id", "price", "quantity", "total_cost"
                ]
            }
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "customer_id"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "store_id"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "sales_date"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "price"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "quantity"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "price",
                "min_value": 0,
                "max_value": 100000
            }
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "quantity",
                "min_value": 0,
                "max_value": 1000
            }
        ),
    ]
    
    for exp in expectations:
        suite.add_expectation(expectation_configuration=exp)
    
    context.save_expectation_suite(suite)
    print(f"✓ Created {suite_name} with {len(expectations)} expectations")
    
    return suite_name


def create_product_expectations(context):
    """Create expectation suite for product data."""
    print("\nCreating product expectations...")
    
    suite_name = "product_suite"
    
    try:
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    except:
        suite = context.get_expectation_suite(expectation_suite_name=suite_name)
    
    from great_expectations.core import ExpectationConfiguration
    
    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "product_id"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "product_name"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "current_price"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "current_price",
                "min_value": 0,
                "max_value": 100000
            }
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "product_id"}
        ),
    ]
    
    for exp in expectations:
        suite.add_expectation(expectation_configuration=exp)
    
    context.save_expectation_suite(suite)
    print(f"✓ Created {suite_name} with {len(expectations)} expectations")
    
    return suite_name


def create_customer_expectations(context):
    """Create expectation suite for customer data."""
    print("\nCreating customer expectations...")
    
    suite_name = "customer_suite"
    
    try:
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    except:
        suite = context.get_expectation_suite(expectation_suite_name=suite_name)
    
    from great_expectations.core import ExpectationConfiguration
    
    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "customer_id"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "email"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={
                "column": "email",
                "regex": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
            }
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "customer_id"}
        ),
    ]
    
    for exp in expectations:
        suite.add_expectation(expectation_configuration=exp)
    
    context.save_expectation_suite(suite)
    print(f"✓ Created {suite_name} with {len(expectations)} expectations")
    
    return suite_name


def create_store_expectations(context):
    """Create expectation suite for store data."""
    print("\nCreating store expectations...")
    
    suite_name = "store_suite"
    
    try:
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    except:
        suite = context.get_expectation_suite(expectation_suite_name=suite_name)
    
    from great_expectations.core import ExpectationConfiguration
    
    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "store_id"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "store_name"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "store_id"}
        ),
    ]
    
    for exp in expectations:
        suite.add_expectation(expectation_configuration=exp)
    
    context.save_expectation_suite(suite)
    print(f"✓ Created {suite_name} with {len(expectations)} expectations")
    
    return suite_name


def main():
    """Setup Great Expectations for the project."""
    print("=" * 70)
    print("GREAT EXPECTATIONS SETUP")
    print("=" * 70)
    
    # Initialize context
    context = setup_ge_context()
    
    # Create expectation suites
    sales_suite = create_sales_expectations(context)
    product_suite = create_product_expectations(context)
    customer_suite = create_customer_expectations(context)
    store_suite = create_store_expectations(context)
    
    print("\n" + "=" * 70)
    print("SETUP COMPLETE!")
    print("=" * 70)
    print("\nExpectation suites created:")
    print(f"  - {sales_suite}")
    print(f"  - {product_suite}")
    print(f"  - {customer_suite}")
    print(f"  - {store_suite}")
    print("\nNext steps:")
    print("  1. Review expectations in gx/expectations/")
    print("  2. Integrate validation into your pipeline")
    print("  3. Run validations with checkpoints")
    print("=" * 70)


if __name__ == "__main__":
    main()
