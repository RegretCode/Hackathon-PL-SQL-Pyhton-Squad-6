"""
PySpark DataFrame converter module.
This module provides a converter that transforms SQL AST to PySpark DataFrame API code.
"""

from typing import List, Optional, Dict, Any
from sql_parser.parser import SQLNode
from converters.base_converter import BaseConverter
import sys
import os
import re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class PySparkConverter(BaseConverter):
    """Converter that transforms SQL AST to PySpark DataFrame API code."""

    def _convert_select_statement(self, node: SQLNode) -> str:
        """Convert a SELECT statement node to PySpark DataFrame API code."""
        from_node = self._find_child_by_type(node, "from_clause")
        where_node = self._find_child_by_type(node, "where_clause")
        select_items_node = self._find_child_by_type(node, "select_items")
        group_by_node = self._find_child_by_type(node, "group_by_clause")
        order_by_node = self._find_child_by_type(node, "order_by_clause")

        code_parts = []
        imports = ["from pyspark.sql.functions import *"]

        # FROM clause with JOINs
        if not from_node or not from_node.children:
            raise ValueError("FROM clause is required")

        # Start with main table
        main_table = from_node.children[0].value
        code_parts.append(f"df = spark.table(\"{main_table}\")")

        # Process JOINs
        for child in from_node.children[1:]:
            if child.node_type == "join":
                join_type = child.attributes.get("type", "JOIN").lower()
                join_table_node = self._find_child_by_type(child, "table")
                join_condition_node = self._find_child_by_type(child, "join_condition")
                
                if join_table_node and join_condition_node:
                    join_table = join_table_node.value
                    condition = join_condition_node.value
                    
                    # Create join DataFrame
                    code_parts.append(f"df2 = spark.table(\"{join_table}\")")
                    
                    # Convert join condition
                    pyspark_condition = self._convert_join_condition(condition)
                    
                    if "left" in join_type:
                        code_parts.append(f"df = df.join(df2, {pyspark_condition}, 'left')")
                    elif "right" in join_type:
                        code_parts.append(f"df = df.join(df2, {pyspark_condition}, 'right')")
                    else:
                        code_parts.append(f"df = df.join(df2, {pyspark_condition})")

        # WHERE clause
        if where_node and where_node.children:
            condition = where_node.children[0].value
            code_parts.append(f"df = df.filter(\"{condition}\")")

        # GROUP BY clause
        if group_by_node and group_by_node.children:
            group_cols = [f"\"{child.value}\"" for child in group_by_node.children]
            code_parts.append(f"df = df.groupBy({', '.join(group_cols)})")
            
            # Process aggregations from SELECT
            if select_items_node:
                agg_exprs = []
                for child in select_items_node.children:
                    if child.node_type == "select_item":
                        item = child.value
                        if "COUNT(" in item.upper():
                            col_match = re.search(r'COUNT\(([^)]+)\)', item, re.IGNORECASE)
                            if col_match:
                                col = col_match.group(1)
                                alias = item.split(' as ')[-1] if ' as ' in item.lower() else item
                                agg_exprs.append(f'count("{col}").alias("{alias}")')
                        elif "AVG(" in item.upper():
                            col_match = re.search(r'AVG\(([^)]+)\)', item, re.IGNORECASE)
                            if col_match:
                                col = col_match.group(1)
                                alias = item.split(' as ')[-1] if ' as ' in item.lower() else item
                                agg_exprs.append(f'avg("{col}").alias("{alias}")')
                
                if agg_exprs:
                    code_parts.append(f"df = df.agg({', '.join(agg_exprs)})")
        else:
            # SELECT clause (no GROUP BY)
            if select_items_node and select_items_node.children:
                select_exprs = []
                for child in select_items_node.children:
                    if child.node_type == "select_item":
                        select_exprs.append(f'"{child.value}"')
                    elif child.node_type == "window_function":
                        window_expr = self._convert_window_function(child)
                        select_exprs.append(window_expr)
                
                code_parts.append(f"df = df.select({', '.join(select_exprs)})")

        # ORDER BY clause
        if order_by_node and order_by_node.children:
            order_exprs = []
            for child in order_by_node.children:
                direction = child.attributes.get("direction", "ASC")
                col = child.value
                if direction == "DESC":
                    order_exprs.append(f'desc("{col}")')
                else:
                    order_exprs.append(f'asc("{col}")')
            
            code_parts.append(f"df = df.orderBy({', '.join(order_exprs)})")

        return "\n".join(imports + [""] + code_parts)

    def _convert_join_condition(self, condition: str) -> str:
        """Convert JOIN condition to PySpark format."""
        # Simple conversion for basic equality joins
        # e.g., "f.departamento_id = d.id" -> "col('f.departamento_id') == col('d.id')"
        if '=' in condition:
            parts = condition.split('=', 1)
            left = parts[0].strip()
            right = parts[1].strip()
            return f"col('{left}') == col('{right}')"
        return f"expr('{condition}')"
    
    def _convert_window_function(self, window_node: SQLNode) -> str:
        """Convert window function to PySpark expression."""
        function_node = self._find_child_by_type(window_node, "function")
        partition_node = self._find_child_by_type(window_node, "partition_by")
        order_node = self._find_child_by_type(window_node, "order_by")
        
        if not function_node:
            return "col('unknown')"
        
        func_name = function_node.value.lower()
        
        # Build window spec
        window_spec_parts = []
        if partition_node:
            partition_cols = [f"col('{child.value}')" for child in partition_node.children]
            window_spec_parts.append(f"partitionBy({', '.join(partition_cols)})")
        
        if order_node:
            order_exprs = []
            for child in order_node.children:
                direction = child.attributes.get("direction", "ASC")
                col_name = child.value
                if direction == "DESC":
                    order_exprs.append(f"col('{col_name}').desc()")
                else:
                    order_exprs.append(f"col('{col_name}')")
            window_spec_parts.append(f"orderBy({', '.join(order_exprs)})")
        
        window_spec = f"Window.{'.'.join(window_spec_parts)}" if window_spec_parts else "Window"
        
        return f"{func_name}().over({window_spec})"
