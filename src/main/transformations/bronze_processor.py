from pyspark.sql import DataFrame
from pyspark.sql.functions import when, lit, current_date


def process_bronze_layer(df: DataFrame, rules: dict, entity_name: str):
    """
    Generic bronze processor - validates and splits records.
    
    Args:
        df: Input DataFrame
        rules: Dict of rule_name -> {"condition": Column}
        entity_name: Name of entity (sales, product, etc.)
    
    Returns:
        Tuple of (valid_df, rejected_df)
    """
    conditions = []
    rejection_expr = None

    for rule_name, rule in rules.items():
        condition = rule["condition"]
        conditions.append(condition)
        if rejection_expr is None:
            rejection_expr = when(~condition, lit(rule_name))
        else:
            rejection_expr = rejection_expr.when(~condition, lit(rule_name))

    final_valid_condition = conditions[0]
    for cond in conditions[1:]:
        final_valid_condition = final_valid_condition & cond

    valid_df = df.filter(final_valid_condition)\
                 .withColumn("record_status", lit("valid"))
    
    rejected_df = df.filter(~final_valid_condition)\
                    .withColumn("rejection_reason", rejection_expr)\
                    .withColumn("record_status", lit("rejected"))

    return valid_df, rejected_df
