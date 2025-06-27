"""
Base converter module.
This module provides a base class for converters that transform SQL AST to Spark code.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_parser.parser import SQLNode
from typing import Any, Dict, List, Optional


class BaseConverter:
    """Base class for SQL AST to Spark code converters."""

    def __init__(self):
        """Initialize the converter."""
        pass

    def convert(self, ast: SQLNode) -> str:
        """
        Convert a SQL AST to Spark code.

        Args:
            ast: The SQL AST to convert.

        Returns:
            The generated Spark code as a string.
        """
        if ast.node_type != "query":
            raise ValueError("Expected a query node as the root of the AST")

        # Process each statement in the query
        result = []
        for statement_node in ast.children:
            statement_code = self._convert_statement(statement_node)
            result.append(statement_code)

        return "\n\n".join(result)

    def _convert_statement(self, node: SQLNode) -> str:
        """
        Convert a statement node to Spark code.

        Args:
            node: The statement node to convert.

        Returns:
            The generated Spark code for the statement.
        """
        if node.node_type == "select_statement":
            return self._convert_select_statement(node)
        else:
            raise ValueError(f"Unsupported statement type: {node.node_type}")

    def _convert_select_statement(self, node: SQLNode) -> str:
        """
        Convert a SELECT statement node to Spark code.

        Args:
            node: The SELECT statement node to convert.

        Returns:
            The generated Spark code for the SELECT statement.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def _find_child_by_type(self, node: SQLNode, node_type: str) -> Optional[SQLNode]:
        """
        Find a child node of the given type.

        Args:
            node: The parent node.
            node_type: The type of child node to find.

        Returns:
            The child node if found, None otherwise.
        """
        for child in node.children:
            if child.node_type == node_type:
                return child
        return None