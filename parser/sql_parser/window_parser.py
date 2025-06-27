"""
Window functions parser module.
This module handles parsing SQL window functions.
"""

import re
from typing import List, Optional, Tuple, Any


class WindowFunctionParser:
    """Parser for SQL window functions."""

    @staticmethod
    def parse_window_function(expression: str) -> Optional[Any]:
        """
        Parse a window function expression.

        Args:
            expression: The window function expression to parse.

        Returns:
            A SQLNode representing the window function, or None if the expression is not a window function.
        """
        # Check if the expression contains OVER keyword, which indicates a window function
        if " OVER " not in expression.upper():
            return None

        # Split the expression into the function part and the window specification
        parts = expression.split(" OVER ", 1)
        if len(parts) != 2:
            return None

        function_part = parts[0].strip()
        window_spec = parts[1].strip()

        # Import SQLNode here to avoid circular import
        from .parser import SQLNode

        # Create the window function node
        window_node = SQLNode("window_function")

        # Parse the function part
        function_node = WindowFunctionParser._parse_function(function_part)
        if function_node:
            window_node.add_child(function_node)

        # Parse the window specification
        if window_spec.startswith("(") and window_spec.endswith(")"):
            window_spec = window_spec[1:-1].strip()

        partition_node, order_node = WindowFunctionParser._parse_window_spec(
            window_spec)

        if partition_node:
            window_node.add_child(partition_node)
        if order_node:
            window_node.add_child(order_node)

        return window_node

    @staticmethod
    def _parse_function(function_part: str) -> Optional[Any]:
        """
        Parse the function part of a window function.

        Args:
            function_part: The function part to parse.

        Returns:
            A SQLNode representing the function.
        """
        # Extract the function name and arguments
        match = re.match(r"(\w+)\((.*)\)", function_part)
        if not match:
            return None

        function_name = match.group(1)
        arguments = match.group(2)

        # Import SQLNode here to avoid circular import
        from .parser import SQLNode
        function_node = SQLNode("function", function_name)

        # Add the arguments as children
        if arguments:
            for arg in arguments.split(","):
                arg = arg.strip()
                if arg:
                    arg_node = SQLNode("argument", arg)
                    function_node.add_child(arg_node)

        return function_node

    @staticmethod
    def _parse_window_spec(window_spec: str) -> Tuple[Optional[Any], Optional[Any]]:
        """
        Parse the window specification part of a window function.

        Args:
            window_spec: The window specification to parse.

        Returns:
            A tuple of (partition_node, order_node), where either may be None if not present.
        """
        partition_node = None
        order_node = None

        # Import SQLNode here to avoid circular import
        from .parser import SQLNode

        # Parse PARTITION BY clause
        partition_match = re.search(
            r"PARTITION\s+BY\s+([\w\s,]+)", window_spec, re.IGNORECASE)
        if partition_match:
            partition_by = partition_match.group(1).strip()
            partition_node = SQLNode("partition_by")

            for column in partition_by.split(","):
                column = column.strip()
                if column:
                    column_node = SQLNode("partition_column", column)
                    partition_node.add_child(column_node)

        # Parse ORDER BY clause
        order_match = re.search(r"ORDER\s+BY\s+([\w\s,]+(?:\s+(?:ASC|DESC))?(?:\s*,\s*[\w\s]+(?:\s+(?:ASC|DESC))?)*)",
                                window_spec, re.IGNORECASE)
        if order_match:
            order_by = order_match.group(1).strip()
            order_node = SQLNode("order_by")

            for column_spec in order_by.split(","):
                column_spec = column_spec.strip()
                if column_spec:
                    direction = "ASC"  # Default direction

                    # Check if there's an ASC or DESC specifier
                    if " DESC" in column_spec.upper():
                        direction = "DESC"
                        column_spec = column_spec.replace(
                            " DESC", "").replace(" desc", "").strip()
                    elif " ASC" in column_spec.upper():
                        column_spec = column_spec.replace(
                            " ASC", "").replace(" asc", "").strip()

                    column_node = SQLNode("order_column", column_spec)
                    column_node.attributes["direction"] = direction
                    order_node.add_child(column_node)

        return partition_node, order_node
