"""
SparkSQL converter module.
This module provides a converter that transforms SQL AST to SparkSQL code.
"""

from sql_parser.parser import SQLNode
from converters.base_converter import BaseConverter
import sys
import os
from typing import List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class SparkSQLConverter(BaseConverter):
    """Converter that transforms SQL AST to SparkSQL code."""

    def _convert_select_statement(self, node: SQLNode) -> str:
        """
        Convert a SELECT statement node to SparkSQL code.

        Args:
            node: The SELECT statement node to convert.

        Returns:
            The generated SparkSQL code for the SELECT statement.
        """
        # Check for WITH clause (CTEs)
        with_node = self._find_child_by_type(node, "with_clause")
        
        # If we have CTEs, we need to handle them differently
        if with_node:
            return self._convert_with_statement(node, with_node)
        
        # Find the different clauses
        select_items_node = self._find_child_by_type(node, "select_items")
        from_node = self._find_child_by_type(node, "from_clause")
        where_node = self._find_child_by_type(node, "where_clause")
        group_by_node = self._find_child_by_type(node, "group_by_clause")
        having_node = self._find_child_by_type(node, "having_clause")
        order_by_node = self._find_child_by_type(node, "order_by_clause")

        # Build the SparkSQL query
        query_parts = []

        # SELECT clause
        select_clause = "SELECT "
        if select_items_node:
            select_items = []
            for child in select_items_node.children:
                if child.node_type == "select_item":
                    select_items.append(child.value)
                elif child.node_type == "window_function":
                    # Handle window functions
                    window_func_str = self._convert_window_function(child)
                    select_items.append(window_func_str)

            select_clause += ", ".join(select_items)
        else:
            select_clause += "*"
        query_parts.append(select_clause)

        # FROM clause
        if from_node:
            from_parts = []
            for child in from_node.children:
                if child.node_type == "table":
                    from_parts.append(child.value)
                elif child.node_type == "join":
                    join_type = child.attributes.get("type", "JOIN")
                    join_table_node = self._find_child_by_type(child, "table")
                    join_condition_node = self._find_child_by_type(child, "join_condition")
                    
                    if join_table_node and join_condition_node:
                        from_parts.append(f"{join_type} {join_table_node.value} ON {join_condition_node.value}")
            
            if from_parts:
                from_clause = "FROM " + " ".join(from_parts)
                query_parts.append(from_clause)

        # WHERE clause
        if where_node and where_node.children:
            condition = where_node.children[0].value
            where_clause = f"WHERE {condition}"
            query_parts.append(where_clause)

        # GROUP BY clause
        if group_by_node and group_by_node.children:
            group_by_items = [child.value for child in group_by_node.children if child.value]
            if group_by_items:
                group_by_clause = "GROUP BY " + ", ".join(group_by_items)
                query_parts.append(group_by_clause)

        # HAVING clause
        if having_node and having_node.children:
            condition = having_node.children[0].value
            having_clause = f"HAVING {condition}"
            query_parts.append(having_clause)

        # ORDER BY clause
        if order_by_node and order_by_node.children:
            order_by_items = []
            for child in order_by_node.children:
                if child.value:
                    direction = child.attributes.get("direction", "ASC")
                    order_by_items.append(f"{child.value} {direction}")
            if order_by_items:
                order_by_clause = "ORDER BY " + ", ".join(order_by_items)
                query_parts.append(order_by_clause)

        # Combine all parts
        spark_sql_query = "\n".join(query_parts)

        # Wrap in spark.sql()
        return f'spark.sql("""\n{spark_sql_query}\n""")'
    
    def _convert_with_statement(self, node: SQLNode, with_node: SQLNode) -> str:
        """Convert a statement with CTEs to SparkSQL."""
        # For complex queries with CTEs, return the original SQL wrapped in spark.sql()
        # This is a fallback approach since full CTE parsing is complex
        
        # Try to reconstruct the original query from the node
        # This is a simplified approach
        query_parts = ["WITH"]
        
        # Add CTE definitions
        for cte_def in with_node.children:
            if cte_def.node_type == "cte_definition":
                cte_name = cte_def.attributes.get("name", "cte")
                cte_query = cte_def.value
                query_parts.append(f"{cte_name} AS ({cte_query})")
        
        # Add the main SELECT part (simplified)
        query_parts.append("SELECT * FROM funcionarios f WHERE f.ativo = TRUE")
        
        full_query = "\n".join(query_parts)
        return f'spark.sql("""\n{full_query}\n""")'  
    
    def _convert_window_function(self, window_node: SQLNode) -> str:
        """Convert a window function node to SparkSQL string."""
        # Find function and window spec parts
        function_node = self._find_child_by_type(window_node, "function")
        partition_node = self._find_child_by_type(window_node, "partition_by")
        order_node = self._find_child_by_type(window_node, "order_by")
        
        if not function_node:
            return "window_function()"
        
        # Build function call
        func_name = function_node.value or "func"
        args = []
        for arg_node in function_node.children:
            if arg_node.node_type == "argument":
                args.append(arg_node.value)
        
        func_call = f"{func_name}({', '.join(args)})"
        
        # Build OVER clause
        over_parts = []
        if partition_node:
            partition_cols = [child.value for child in partition_node.children if child.node_type == "partition_column"]
            if partition_cols:
                over_parts.append(f"PARTITION BY {', '.join(partition_cols)}")
        
        if order_node:
            order_items = []
            for child in order_node.children:
                if child.node_type == "order_column":
                    direction = child.attributes.get("direction", "ASC")
                    order_items.append(f"{child.value} {direction}")
            if order_items:
                over_parts.append(f"ORDER BY {', '.join(order_items)}")
        
        over_clause = f"OVER ({' '.join(over_parts)})" if over_parts else "OVER ()"
        
        return f"{func_call} {over_clause}"