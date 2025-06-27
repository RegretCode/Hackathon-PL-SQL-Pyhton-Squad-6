"""
SQL Parser module.
This module handles parsing SQL queries into Abstract Syntax Trees (AST).
"""

import sqlparse
import logging
import re
from typing import Dict, Any, List, Optional, Union, Tuple
from .window_parser import WindowFunctionParser
from .advanced_clauses import AdvancedClausesParser
from .postgres_extensions import PostgresExtensionsParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SQLNode:
    """Base class for SQL AST nodes."""

    def __init__(self, node_type: str, value: Optional[str] = None):
        self.node_type = node_type
        self.value = value
        self.children = []
        self.parent = None
        self.attributes = {}

    def add_child(self, node: 'SQLNode') -> None:
        """Add a child node to this node."""
        self.children.append(node)
        node.parent = self

    def to_dict(self) -> Dict[str, Any]:
        """Convert the node to a dictionary representation."""
        result = {
            'type': self.node_type,
        }

        if self.value is not None:
            result['value'] = self.value

        if self.attributes:
            result['attributes'] = self.attributes

        if self.children:
            result['children'] = [child.to_dict() for child in self.children]

        return result


class SQLParser:
    """Parser for SQL queries that produces an AST."""

    def __init__(self):
        """Initialize the SQL parser."""
        pass

    def parse(self, sql_query: str) -> SQLNode:
        """
        Parse a SQL query into an AST.

        Args:
            sql_query: The SQL query to parse.

        Returns:
            A SQLNode representing the root of the AST.
        """
        # Parse the SQL query using sqlparse
        parsed = sqlparse.parse(sql_query)
        if not parsed:
            raise ValueError("Failed to parse SQL query")

        # Create the root node
        root = SQLNode("query")

        # Process each statement in the parsed SQL
        for statement in parsed:
            statement_node = self._process_statement(statement)
            root.add_child(statement_node)

        return root

    def _process_statement(self, statement) -> SQLNode:
        """Process a SQL statement and convert it to an AST node."""
        # Determine the statement type
        if statement.get_type() == "SELECT":
            return self._process_select_statement(statement)
        else:
            # For now, we'll just create a generic node for other statement types
            node = SQLNode("statement", statement.get_type())
            return node

    def _process_select_statement(self, statement) -> SQLNode:
        """Process a SELECT statement and convert it to an AST node."""
        select_node = SQLNode("select_statement")
        
        # Check if this is a CTE (WITH clause)
        statement_str = str(statement).strip()
        if statement_str.upper().startswith('WITH'):
            cte_node = self._process_cte(statement_str)
            if cte_node:
                select_node.add_child(cte_node)

        # Process all clauses
        select_items_node = self._process_select_items(statement)
        if select_items_node.children:
            select_node.add_child(select_items_node)

        from_node = self._process_from_clause(statement)
        if from_node.children:
            select_node.add_child(from_node)

        where_node = self._process_where_clause(statement)
        if where_node.children:
            select_node.add_child(where_node)
            
        group_by_node = self._process_group_by_clause(statement)
        if group_by_node.children:
            select_node.add_child(group_by_node)
            
        order_by_node = self._process_order_by_clause(statement)
        if order_by_node.children:
            select_node.add_child(order_by_node)

        return select_node

    def _process_select_items(self, statement) -> SQLNode:
        """Process the SELECT items in a SELECT statement."""
        select_items_node = SQLNode("select_items")

        # Look for IdentifierList or Identifier tokens
        for token in statement.tokens:
            if (hasattr(token, '__class__') and
                    (token.__class__.__name__ == 'IdentifierList' or token.__class__.__name__ == 'Identifier')):
                # This is the select items
                items_str = str(token).strip()
                if items_str:
                    # Split by comma to get individual select items
                    items_list = [item.strip()
                                  for item in items_str.split(',')]
                    for item_str in items_list:
                        if item_str:
                            # Check if this is a window function
                            window_node = WindowFunctionParser.parse_window_function(item_str)
                            if window_node:
                                select_items_node.add_child(window_node)
                            else:
                                item_node = SQLNode("select_item", item_str)
                                select_items_node.add_child(item_node)
                break

        return select_items_node

    def _process_from_clause(self, statement) -> SQLNode:
        """Process the FROM clause including JOINs."""
        from_node = SQLNode("from_clause")
        statement_str = str(statement).upper()
        
        # Extract FROM clause with JOINs
        from_match = re.search(r'FROM\s+(.+?)(?:\s+WHERE|\s+GROUP\s+BY|\s+ORDER\s+BY|\s+HAVING|$)', statement_str, re.DOTALL)
        if from_match:
            from_clause = from_match.group(1).strip()
            
            # Split by JOIN keywords to identify tables and joins
            join_parts = re.split(r'\s+((?:LEFT\s+|RIGHT\s+|INNER\s+|FULL\s+)?JOIN)\s+', from_clause)
            
            # First part is the main table
            if join_parts:
                main_table = join_parts[0].strip()
                table_node = SQLNode("table", main_table)
                from_node.add_child(table_node)
                
                # Process JOINs
                for i in range(1, len(join_parts), 2):
                    if i + 1 < len(join_parts):
                        join_type = join_parts[i].strip()
                        join_clause = join_parts[i + 1].strip()
                        
                        join_node = SQLNode("join")
                        join_node.attributes["type"] = join_type
                        
                        # Extract table and condition
                        on_match = re.search(r'(.+?)\s+ON\s+(.+)', join_clause)
                        if on_match:
                            table_name = on_match.group(1).strip()
                            condition = on_match.group(2).strip()
                            
                            join_table_node = SQLNode("table", table_name)
                            join_condition_node = SQLNode("join_condition", condition)
                            
                            join_node.add_child(join_table_node)
                            join_node.add_child(join_condition_node)
                            from_node.add_child(join_node)

        return from_node

    def _process_where_clause(self, statement) -> SQLNode:
        """Process the WHERE clause in a SELECT statement."""
        where_node = SQLNode("where_clause")

        # Look for Where token
        for token in statement.tokens:
            if hasattr(token, '__class__') and token.__class__.__name__ == 'Where':
                # Extract the condition (remove "WHERE " prefix)
                condition_str = str(token).strip()
                if condition_str.upper().startswith('WHERE '):
                    condition_str = condition_str[6:].strip()
                if condition_str:
                    condition_node = SQLNode("condition", condition_str)
                    where_node.add_child(condition_node)
                break

        return where_node
    
    def _process_cte(self, statement_str: str) -> Optional[SQLNode]:
        """Process Common Table Expressions (WITH clause)."""
        cte_node = SQLNode("with_clause")
        
        # Simple regex to extract CTE definitions
        # This is a basic implementation - could be improved
        with_pattern = r'WITH\s+(\w+)\s+AS\s*\((.*?)\)'
        matches = re.finditer(with_pattern, statement_str, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            cte_name = match.group(1)
            cte_query = match.group(2).strip()
            
            cte_def_node = SQLNode("cte_definition")
            cte_def_node.attributes["name"] = cte_name
            cte_def_node.value = cte_query
            cte_node.add_child(cte_def_node)
        
        return cte_node if cte_node.children else None
    
    def _process_group_by_clause(self, statement) -> SQLNode:
        """Process the GROUP BY clause."""
        group_by_node = SQLNode("group_by_clause")
        statement_str = str(statement).upper()
        
        group_by_match = re.search(r'GROUP\s+BY\s+([^\s]+(?:\s*,\s*[^\s]+)*)', statement_str)
        if group_by_match:
            group_by_items = group_by_match.group(1).split(',')
            for item in group_by_items:
                item = item.strip()
                if item:
                    group_item_node = SQLNode("group_by_item", item)
                    group_by_node.add_child(group_item_node)
        
        return group_by_node
    
    def _process_order_by_clause(self, statement) -> SQLNode:
        """Process the ORDER BY clause."""
        order_by_node = SQLNode("order_by_clause")
        statement_str = str(statement).upper()
        
        order_by_match = re.search(r'ORDER\s+BY\s+(.+?)(?:\s+LIMIT|$)', statement_str, re.DOTALL)
        if order_by_match:
            order_by_items = order_by_match.group(1).split(',')
            for item in order_by_items:
                item = item.strip()
                if item:
                    # Check for ASC/DESC
                    direction = "ASC"
                    if item.endswith(' DESC'):
                        direction = "DESC"
                        item = item[:-5].strip()
                    elif item.endswith(' ASC'):
                        item = item[:-4].strip()
                    
                    order_item_node = SQLNode("order_by_item", item)
                    order_item_node.attributes["direction"] = direction
                    order_by_node.add_child(order_item_node)
        
        return order_by_node
