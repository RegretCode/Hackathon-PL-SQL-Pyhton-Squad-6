"""
PostgreSQL extensions parser module.
This module handles parsing PostgreSQL-specific SQL features.
"""

import re
from typing import Optional, List, Dict, Any


class PostgresExtensionsParser:
    """Parser for PostgreSQL-specific SQL features."""

    @staticmethod
    def parse_array_access(expression: str) -> Optional[Any]:
        """
        Parse array access expressions in PostgreSQL.

        Args:
            expression: The expression to parse.

        Returns:
            A SQLNode representing the array access, or None if the expression is not an array access.
        """
        # Match array access patterns like column[1] or column[1:3]
        match = re.search(r'(\w+)\[(\d+)(?::(\d+))?\]', expression)
        if match:
            # Import SQLNode here to avoid circular import
            from .parser import SQLNode
            array_node = SQLNode("array_access")

            # The array name
            array_name = match.group(1)
            array_node.attributes["array"] = array_name

            # The array index or slice
            start_idx = match.group(2)
            end_idx = match.group(3) if match.group(3) else None

            if end_idx:
                # This is a slice
                array_node.attributes["slice"] = True
                array_node.attributes["start"] = start_idx
                array_node.attributes["end"] = end_idx
            else:
                # This is a single index
                array_node.attributes["index"] = start_idx

            return array_node

        return None

    @staticmethod
    def parse_json_operators(expression: str) -> Optional[Any]:
        """
        Parse JSON operators in PostgreSQL.

        Args:
            expression: The expression to parse.

        Returns:
            A SQLNode representing the JSON operation, or None if the expression is not a JSON operation.
        """
        # Match JSON operators like column->>'key' or column->'key'
        match = re.search(r'(\w+)\s*(->>?)\s*\'([^\']+)\'', expression)
        if match:
            # Import SQLNode here to avoid circular import
            from .parser import SQLNode
            json_node = SQLNode("json_operation")

            # The column/object name
            column_name = match.group(1)
            json_node.attributes["column"] = column_name

            # The operator type
            operator = match.group(2)
            json_node.attributes["operator"] = operator

            # The key
            key = match.group(3)
            json_node.attributes["key"] = key

            # Determine if text extraction (->>)
            if operator == '->>':
                json_node.attributes["extract_text"] = True

            return json_node

        return None

    @staticmethod
    def parse_window_frame(frame_clause: str) -> Dict[str, Any]:
        """
        Parse window frame clauses in PostgreSQL.

        Args:
            frame_clause: The frame clause to parse.

        Returns:
            A dictionary with window frame attributes.
        """
        frame_attrs = {}

        # Determine frame type (ROWS or RANGE)
        if "ROWS" in frame_clause.upper():
            frame_attrs["type"] = "ROWS"
        elif "RANGE" in frame_clause.upper():
            frame_attrs["type"] = "RANGE"
        else:
            return frame_attrs  # No valid frame type found

        # Parse frame bounds
        if "UNBOUNDED PRECEDING" in frame_clause.upper():
            frame_attrs["start"] = "UNBOUNDED PRECEDING"
        elif "CURRENT ROW" in frame_clause.upper() and not "BETWEEN" in frame_clause.upper():
            frame_attrs["start"] = "CURRENT ROW"

        # Parse BETWEEN ... AND ... format
        between_match = re.search(
            r'BETWEEN\s+(.+?)\s+AND\s+(.+)', frame_clause, re.IGNORECASE)
        if between_match:
            start_bound = between_match.group(1).strip()
            end_bound = between_match.group(2).strip()

            frame_attrs["start"] = start_bound
            frame_attrs["end"] = end_bound

        return frame_attrs

    @staticmethod
    def parse_cast_expression(expression: str) -> Optional[Any]:
        """
        Parse CAST expressions in PostgreSQL.

        Args:
            expression: The expression to parse.

        Returns:
            A SQLNode representing the CAST, or None if the expression is not a CAST.
        """
        # Match CAST expressions like CAST(column AS type)
        cast_match = re.search(
            r'CAST\s*\(\s*(.+?)\s+AS\s+([^)]+)\)', expression, re.IGNORECASE)
        if cast_match:
            # Import SQLNode here to avoid circular import
            from .parser import SQLNode
            cast_node = SQLNode("cast_expression")

            # The expression being cast
            expr = cast_match.group(1).strip()
            cast_node.attributes["expression"] = expr

            # The target type
            target_type = cast_match.group(2).strip()
            cast_node.attributes["type"] = target_type

            return cast_node

        # Match PostgreSQL :: cast operator like column::type
        pg_cast_match = re.search(r'(.+?)::\s*([^\s,)]+)', expression)
        if pg_cast_match:
            # Import SQLNode here to avoid circular import
            from .parser import SQLNode
            cast_node = SQLNode("cast_expression")

            # The expression being cast
            expr = pg_cast_match.group(1).strip()
            cast_node.attributes["expression"] = expr

            # The target type
            target_type = pg_cast_match.group(2).strip()
            cast_node.attributes["type"] = target_type
            cast_node.attributes["pg_style"] = True

            return cast_node

        return None
