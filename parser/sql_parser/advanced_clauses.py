"""
Advanced SQL clauses parser module.
This module handles parsing advanced SQL clauses like DISTINCT, UNION, LIMIT, and CTEs.
"""

import re
from typing import Optional, List, Any


class AdvancedClausesParser:
    """Parser for advanced SQL clauses."""

    @staticmethod
    def parse_distinct(select_clause: str) -> bool:
        """
        Check if a SELECT clause contains the DISTINCT keyword.

        Args:
            select_clause: The SELECT clause to check.

        Returns:
            True if the SELECT clause contains DISTINCT, False otherwise.
        """
        return "DISTINCT" in select_clause.upper().split(None, 2)[:2]

    @staticmethod
    def parse_limit(sql_query: str) -> Optional[int]:
        """
        Parse the LIMIT clause from a SQL query.

        Args:
            sql_query: The SQL query to parse.

        Returns:
            The limit value as an integer, or None if no LIMIT clause is found.
        """
        match = re.search(r"LIMIT\s+(\d+)", sql_query, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def parse_cte(sql_query: str) -> List[Any]:
        """
        Parse Common Table Expressions (CTEs) from a SQL query.

        Args:
            sql_query: The SQL query to parse.

        Returns:
            A list of CTE nodes, or an empty list if no CTEs are found.
        """
        # Check if the query starts with a WITH clause
        if not sql_query.upper().strip().startswith("WITH"):
            return []

        # Extract the WITH clause
        with_match = re.match(r"WITH\s+(.*?)(?=SELECT|UPDATE|DELETE|INSERT)",
                              sql_query, re.IGNORECASE | re.DOTALL)
        if not with_match:
            return []

        with_clause = with_match.group(1).strip()

        # Parse each CTE
        cte_nodes = []

        # Split the WITH clause by commas that are not inside parentheses
        paren_level = 0
        start_idx = 0
        cte_strings = []

        for i, char in enumerate(with_clause):
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
            elif char == ',' and paren_level == 0:
                cte_strings.append(with_clause[start_idx:i].strip())
                start_idx = i + 1

        # Add the last CTE
        if start_idx < len(with_clause):
            cte_strings.append(with_clause[start_idx:].strip())

        # Process each CTE string
        for cte_str in cte_strings:
            # Extract the CTE name and definition
            cte_match = re.match(
                r"(\w+)(?:\s*\(.*?\))?\s+AS\s+\((.*)\)", cte_str, re.IGNORECASE | re.DOTALL)
            if cte_match:
                cte_name = cte_match.group(1)
                cte_query = cte_match.group(2).strip()

                # Import SQLNode here to avoid circular import
                from .parser import SQLNode
                cte_node = SQLNode("cte", cte_name)
                cte_node.attributes["query"] = cte_query
                cte_nodes.append(cte_node)

        return cte_nodes

    @staticmethod
    def parse_union(sql_query: str) -> List[str]:
        """
        Parse UNION clauses from a SQL query.

        Args:
            sql_query: The SQL query to parse.

        Returns:
            A list of SQL queries that are part of the UNION.
        """
        # Split by UNION keywords that are not inside parentheses
        union_parts = []
        current_part = ""
        paren_level = 0
        i = 0

        while i < len(sql_query):
            # Check for UNION keyword
            if (sql_query[i:i+5].upper() == "UNION" and paren_level == 0 and
                    (i == 0 or sql_query[i-1].isspace())):
                # Add the current part if not empty
                if current_part.strip():
                    union_parts.append(current_part.strip())
                current_part = ""

                # Skip the UNION keyword
                i += 5

                # Skip "ALL" if present
                if i + 3 < len(sql_query) and sql_query[i:i+4].upper() == " ALL":
                    i += 4
            else:
                if sql_query[i] == '(':
                    paren_level += 1
                elif sql_query[i] == ')':
                    paren_level -= 1

                current_part += sql_query[i]
                i += 1

        # Add the last part
        if current_part.strip():
            union_parts.append(current_part.strip())

        return union_parts
