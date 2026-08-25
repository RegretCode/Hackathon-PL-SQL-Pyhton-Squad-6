"""
SQL Parser Module

A dedicated SQL parsing module that extracts and analyzes SQL query components.
This parser focuses on SELECT statements and breaks them down into their constituent parts. 
"""

import re
from typing import Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum


class SQLClauseType(Enum):
    """Enumeration of supported SQL clause types."""
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    ORDER_BY = "ORDER_BY"
    LIMIT = "LIMIT"
    GROUP_BY = "GROUP_BY"
    HAVING = "HAVING"
    JOIN = "JOIN"
    INNER_JOIN = "INNER_JOIN"
    LEFT_JOIN = "LEFT_JOIN"
    RIGHT_JOIN = "RIGHT_JOIN"


class OperatorType(Enum):
    """Enumeration of supported SQL operators."""
    EQUALS = "="
    NOT_EQUALS = "!="
    NOT_EQUALS_ALT = "<>"
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    LIKE = "LIKE"
    IN = "IN"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


class AggregateFunction(Enum):
    """Enumeration of supported aggregate functions."""
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MAX = "MAX"
    MIN = "MIN"
    DISTINCT = "DISTINCT"


class JoinType(Enum):
    """Enumeration of supported JOIN types."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL JOIN"
    CROSS = "CROSS JOIN"


@dataclass
class SelectColumn:
    """Represents a column in the SELECT clause."""
    name: str
    alias: Optional[str] = None
    is_wildcard: bool = False
    is_aggregate: bool = False
    aggregate_function: Optional[str] = None
    
    def __str__(self):
        if self.is_wildcard:
            return "*"
        if self.is_aggregate:
            base = f"{self.aggregate_function}({self.name})"
            if self.alias:
                return f"{base} AS {self.alias}"
            return base
        if self.alias:
            return f"{self.name} AS {self.alias}"
        return self.name


@dataclass
class WhereCondition:
    """Represents a condition in the WHERE clause."""
    column: str
    operator: str
    value: Union[str, int, float]
    logical_operator: Optional[str] = None  # AND, OR
    
    def __str__(self):
        return f"{self.column} {self.operator} {self.value}"


@dataclass
class OrderByColumn:
    """Represents a column in the ORDER BY clause."""
    column: str
    direction: str = "ASC"  # ASC or DESC
    
    def __str__(self):
        return f"{self.column} {self.direction}"


@dataclass
class JoinClause:
    """Represents a JOIN clause."""
    join_type: str  # INNER, LEFT, RIGHT, etc.
    table: str
    condition: str
    
    def __str__(self):
        return f"{self.join_type} {self.table} ON {self.condition}"


@dataclass
class GroupByColumn:
    """Represents a column in the GROUP BY clause."""
    column: str
    
    def __str__(self):
        return self.column


@dataclass
class ParsedSQL:
    """Container for parsed SQL query components."""
    original_query: str
    select_columns: List[SelectColumn]
    from_table: str
    where_conditions: List[WhereCondition]
    order_by_columns: List[OrderByColumn]
    join_clauses: List[JoinClause]
    group_by_columns: List[GroupByColumn]
    having_conditions: List[WhereCondition]
    limit_count: Optional[int] = None
    is_valid: bool = True
    errors: List[str] = None
    has_aggregates: bool = False
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        # Check if query has aggregate functions
        self.has_aggregates = any(col.is_aggregate for col in self.select_columns)


class SQLParser:
    """
    SQL Parser class that analyzes and breaks down SQL SELECT statements.
    
    This parser uses regex patterns to extract different components of SQL queries
    and provides structured access to the parsed elements.
    """
    
    def __init__(self):
        """Initialize the SQL parser with regex patterns."""
        self.patterns = {
            'main_query': (
                r"SELECT\s+(?P<select>.*?)\s+FROM\s+(?P<from>\S+(?:\s+\w+)?)"
                r"(?P<joins>(?:\s+(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+\S+(?:\s+\w+)?\s+ON\s+[^;]+?)*?)"
                r"(?:\s+WHERE\s+(?P<where>.*?)(?=\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$))?"
                r"(?:\s+GROUP\s+BY\s+(?P<groupby>.*?)(?=\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|$))?"
                r"(?:\s+HAVING\s+(?P<having>.*?)(?=\s+ORDER\s+BY|\s+LIMIT|$))?"
                r"(?:\s+ORDER\s+BY\s+(?P<orderby>.*?)(?=\s+LIMIT|$))?"
                r"(?:\s+LIMIT\s+(?P<limit>\d+))?"
            ),
            'aggregate_functions': [
                r"(COUNT|SUM|AVG|MAX|MIN)\s*\(\s*([^)]+)\s*\)",
                r"(DISTINCT)\s+(\w+)"
            ],
            'join_pattern': r"(INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+(\S+)(?:\s+(\w+))?\s+ON\s+([^;]+?)(?=\s+(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN|\s+WHERE|\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)",
            'where_operators': [
                (r"(\w+)\s*=\s*'([^']*)'", r"=", "string"),
                (r"(\w+)\s*=\s*(\d+(?:\.\d+)?)", r"=", "number"),
                (r"(\w+)\s*!=\s*'([^']*)'", r"!=", "string"),
                (r"(\w+)\s*!=\s*(\d+(?:\.\d+)?)", r"!=", "number"),
                (r"(\w+)\s*<>\s*'([^']*)'", r"<>", "string"),
                (r"(\w+)\s*<>\s*(\d+(?:\.\d+)?)", r"<>", "number"),
                (r"(\w+)\s*>\s*(\d+(?:\.\d+)?)", r">", "number"),
                (r"(\w+)\s*<\s*(\d+(?:\.\d+)?)", r"<", "number"),
                (r"(\w+)\s*>=\s*(\d+(?:\.\d+)?)", r">=", "number"),
                (r"(\w+)\s*<=\s*(\d+(?:\.\d+)?)", r"<=", "number"),
                (r"(\w+)\s+LIKE\s+'([^']*)'", r"LIKE", "string"),
                (r"(\w+)\s+IN\s*\(([^)]+)\)", r"IN", "list"),
                (r"(\w+)\s+IS\s+NULL", r"IS NULL", "null"),
                (r"(\w+)\s+IS\s+NOT\s+NULL", r"IS NOT NULL", "null"),
            ]
        }
    
    def clean_sql(self, sql_query: str) -> str:
        """
        Clean and normalize the SQL query.
        
        Args:
            sql_query (str): Raw SQL query string
            
        Returns:
            str: Cleaned SQL query
        """
        # Remove leading/trailing whitespace and semicolons
        cleaned = sql_query.strip().rstrip(';')
        
        # Normalize whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned
    
    def parse_select_columns(self, select_clause: str) -> List[SelectColumn]:
        """
        Parse the SELECT clause to extract columns and aliases, including aggregate functions.
        
        Args:
            select_clause (str): The SELECT clause content
            
        Returns:
            List[SelectColumn]: List of parsed select columns
        """
        columns = []
        
        # Handle SELECT *
        if select_clause.strip() == '*':
            columns.append(SelectColumn(name="*", is_wildcard=True))
            return columns
        
        # Split columns by comma (but not inside parentheses)
        column_parts = self._split_columns(select_clause)
        
        for part in column_parts:
            part = part.strip()
            
            # Check for aggregate functions
            aggregate_match = None
            for pattern in self.patterns['aggregate_functions']:
                aggregate_match = re.search(pattern, part, re.IGNORECASE)
                if aggregate_match:
                    break
            
            if aggregate_match:
                # Handle aggregate function
                func_name = aggregate_match.group(1).upper()
                func_arg = aggregate_match.group(2).strip()
                
                # Check for alias after the aggregate function
                alias = None
                remaining_part = part[aggregate_match.end():].strip()
                if remaining_part.lower().startswith('as '):
                    alias = remaining_part[3:].strip()
                elif remaining_part and not remaining_part.startswith(','):
                    # Implicit alias (no AS keyword)
                    alias = remaining_part.strip()
                
                columns.append(SelectColumn(
                    name=func_arg,
                    alias=alias,
                    is_aggregate=True,
                    aggregate_function=func_name
                ))
            else:
                # Handle regular column
                # Check for alias using AS keyword
                if ' as ' in part.lower():
                    col_parts = re.split(r'\s+as\s+', part, flags=re.IGNORECASE)
                    if len(col_parts) == 2:
                        columns.append(SelectColumn(
                            name=col_parts[0].strip(),
                            alias=col_parts[1].strip()
                        ))
                    else:
                        columns.append(SelectColumn(name=part))
                else:
                    columns.append(SelectColumn(name=part))
        
        return columns
    
    def parse_where_conditions(self, where_clause: str) -> List[WhereCondition]:
        """
        Parse the WHERE clause to extract conditions.
        
        Args:
            where_clause (str): The WHERE clause content
            
        Returns:
            List[WhereCondition]: List of parsed where conditions
        """
        conditions = []
        
        # Split by AND/OR while preserving the logical operators
        parts = re.split(r'\s+(AND|OR)\s+', where_clause, flags=re.IGNORECASE)
        
        logical_op = None
        for i, part in enumerate(parts):
            part = part.strip()
            
            if part.upper() in ['AND', 'OR']:
                logical_op = part.upper()
                continue
            
            # Try to match different operator patterns
            condition = self._parse_single_condition(part)
            if condition:
                if logical_op and conditions:
                    # Apply logical operator to the previous condition
                    conditions[-1].logical_operator = logical_op
                conditions.append(condition)
                logical_op = None
        
        return conditions
    
    def parse_order_by_columns(self, orderby_clause: str) -> List[OrderByColumn]:
        """
        Parse the ORDER BY clause to extract columns and directions.
        
        Args:
            orderby_clause (str): The ORDER BY clause content
            
        Returns:
            List[OrderByColumn]: List of parsed order by columns
        """
        columns = []
        
        # Split by comma
        column_parts = [part.strip() for part in orderby_clause.split(',')]
        
        for part in column_parts:
            parts = part.split()
            if len(parts) >= 2:
                column = parts[0]
                direction = parts[1].upper()
                if direction in ['ASC', 'DESC']:
                    columns.append(OrderByColumn(column=column, direction=direction))
                else:
                    columns.append(OrderByColumn(column=column))
            else:
                columns.append(OrderByColumn(column=part))
        
        return columns
    
    def parse(self, sql_query: str) -> ParsedSQL:
        """
        Parse a complete SQL query into its components.
        
        Args:
            sql_query (str): The SQL query to parse
            
        Returns:
            ParsedSQL: Parsed SQL query object
        """
        # Initialize result object
        result = ParsedSQL(
            original_query=sql_query,
            select_columns=[],
            from_table="",
            where_conditions=[],
            order_by_columns=[],
            join_clauses=[],
            group_by_columns=[],
            having_conditions=[],
            errors=[]
        )
        
        try:
            # Clean the SQL
            cleaned_sql = self.clean_sql(sql_query)
            
            # Try a more flexible parsing approach
            # First, extract the main components using individual patterns
            
            # Extract SELECT clause
            select_match = re.search(r"SELECT\s+(.*?)\s+FROM", cleaned_sql, re.IGNORECASE | re.DOTALL)
            if select_match:
                result.select_columns = self.parse_select_columns(select_match.group(1))
            
            # Extract FROM clause and table
            from_match = re.search(r"FROM\s+(\S+(?:\s+\w+)?)", cleaned_sql, re.IGNORECASE)
            if from_match:
                result.from_table = from_match.group(1).strip()
            
            # Extract JOIN clauses directly
            join_pattern = r"((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+\S+(?:\s+\w+)?\s+ON\s+[^;]+?)(?=\s+(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN|\s+WHERE|\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)"
            join_matches = re.findall(join_pattern, cleaned_sql, re.IGNORECASE)
            for join_match in join_matches:
                join_clauses = self.parse_join_clauses(join_match)
                result.join_clauses.extend(join_clauses)
            
            # Extract WHERE clause
            where_match = re.search(r"WHERE\s+(.*?)(?=\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)", cleaned_sql, re.IGNORECASE | re.DOTALL)
            if where_match:
                result.where_conditions = self.parse_where_conditions(where_match.group(1))
            
            # Extract GROUP BY clause
            groupby_match = re.search(r"GROUP\s+BY\s+(.*?)(?=\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|$)", cleaned_sql, re.IGNORECASE | re.DOTALL)
            if groupby_match:
                result.group_by_columns = [GroupByColumn(column=col.strip()) for col in groupby_match.group(1).split(',')]
            
            # Extract HAVING clause
            having_match = re.search(r"HAVING\s+(.*?)(?=\s+ORDER\s+BY|\s+LIMIT|$)", cleaned_sql, re.IGNORECASE | re.DOTALL)
            if having_match:
                result.having_conditions = self.parse_where_conditions(having_match.group(1))
            
            # Extract ORDER BY clause
            orderby_match = re.search(r"ORDER\s+BY\s+(.*?)(?=\s+LIMIT|$)", cleaned_sql, re.IGNORECASE | re.DOTALL)
            if orderby_match:
                result.order_by_columns = self.parse_order_by_columns(orderby_match.group(1))
            
            # Extract LIMIT clause
            limit_match = re.search(r"LIMIT\s+(\d+)", cleaned_sql, re.IGNORECASE)
            if limit_match:
                try:
                    result.limit_count = int(limit_match.group(1))
                except ValueError:
                    result.errors.append(f"Invalid LIMIT value: {limit_match.group(1)}")
            
            # Validate that we got the basic components
            if not result.select_columns or not result.from_table:
                result.is_valid = False
                result.errors.append("Unable to parse basic SQL query structure (SELECT/FROM)")
            
        except Exception as e:
            result.is_valid = False
            result.errors.append(f"Parsing error: {str(e)}")
        
        return result
    
    def _split_columns(self, select_clause: str) -> List[str]:
        """Split columns by comma, respecting parentheses."""
        columns = []
        current_column = ""
        paren_count = 0
        
        for char in select_clause:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                columns.append(current_column.strip())
                current_column = ""
                continue
            
            current_column += char
        
        if current_column.strip():
            columns.append(current_column.strip())
        
        return columns
    
    def _parse_single_condition(self, condition_str: str) -> Optional[WhereCondition]:
        """Parse a single WHERE condition."""
        for pattern, operator, value_type in self.patterns['where_operators']:
            match = re.search(pattern, condition_str, re.IGNORECASE)
            if match:
                column = match.group(1)
                value_str = match.group(2)
                
                # Convert value based on type
                if value_type == "number":
                    try:
                        value = float(value_str) if '.' in value_str else int(value_str)
                    except ValueError:
                        value = value_str
                elif value_type == "string":
                    value = value_str
                elif value_type == "list":
                    # Parse IN clause values
                    value = [v.strip().strip("'\"") for v in value_str.split(',')]
                else:
                    value = value_str
                
                return WhereCondition(
                    column=column,
                    operator=operator,
                    value=value
                )
        
        return None
    
    def parse_join_clauses(self, join_str: str) -> List[JoinClause]:
        """
        Parse a single JOIN clause string to extract join information.
        
        Args:
            join_str (str): A single JOIN clause string
            
        Returns:
            List[JoinClause]: List containing one parsed join clause
        """
        joins = []
        
        if not join_str or not join_str.strip():
            return joins
        
        # Parse individual JOIN clause
        # Pattern: [JOIN_TYPE] JOIN table [alias] ON condition
        join_pattern = r"(INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+(\S+)(?:\s+(\w+))?\s+ON\s+(.*)"
        match = re.search(join_pattern, join_str.strip(), re.IGNORECASE)
        
        if match:
            join_type_raw = match.group(1)
            table = match.group(2)
            table_alias = match.group(3)  # Optional table alias
            condition = match.group(4).strip()
            
            # Determine join type
            if join_type_raw:
                join_type = join_type_raw.strip().upper() + " JOIN"
            else:
                join_type = "INNER JOIN"  # Default JOIN is INNER JOIN
            
            # Include alias if present
            table_with_alias = table
            if table_alias:
                table_with_alias = f"{table} {table_alias}"
            
            joins.append(JoinClause(
                join_type=join_type,
                table=table_with_alias,
                condition=condition
            ))
        
        return joins
    
    def parse_group_by_columns(self, groupby_clause: str) -> List[GroupByColumn]:
        """
        Parse the GROUP BY clause to extract columns.
        
        Args:
            groupby_clause (str): The GROUP BY clause content
            
        Returns:
            List[GroupByColumn]: List of parsed group by columns
        """
        columns = []
        
        if not groupby_clause or not groupby_clause.strip():
            return columns
        
        # Split by comma
        column_parts = [part.strip() for part in groupby_clause.split(',')]
        
        for part in column_parts:
            if part:
                columns.append(GroupByColumn(column=part))
        
        return columns

    def get_parser_info(self) -> Dict[str, any]:
        """Get information about the parser capabilities."""
        return {
            "supported_clauses": [clause.value for clause in SQLClauseType],
            "supported_operators": [op.value for op in OperatorType],
            "patterns_count": len(self.patterns),
            "version": "1.0.0"
        }


def main():
    """Demo function to show parser capabilities."""
    parser = SQLParser()
    
    # Test queries
    test_queries = [
        "SELECT nome, idade FROM usuarios",
        "SELECT nome, idade as user_age FROM usuarios WHERE idade > 18",
        "SELECT * FROM funcionarios WHERE departamento = 'TI' ORDER BY salario DESC",
        "SELECT nome, salario FROM funcionarios WHERE salario >= 50000 AND departamento = 'TI' ORDER BY nome ASC LIMIT 10",
        "SELECT p.nome, p.idade, d.nome FROM pessoas p JOIN departamentos d ON p.depto_id = d.id",
        "SELECT * FROM vendas WHERE produto_id IN (SELECT id FROM produtos WHERE categoria = 'Eletrônicos')",
        "SELECT cliente_id, COUNT(*) as total_vendas FROM vendas GROUP BY cliente_id HAVING total_vendas > 10"
    ]
    
    print("🔍 SQL Parser Demo")
    print("=" * 50)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n📝 Query {i}: {query}")
        print("-" * 50)
        
        result = parser.parse(query)
        
        if not result.is_valid:
            print(f"❌ Parsing failed: {', '.join(result.errors)}")
            continue
        
        print(f"📊 Parsed Components:")
        print(f"   Table: {result.from_table}")
        print(f"   Columns: {[str(col) for col in result.select_columns]}")
        
        if result.where_conditions:
            print(f"   Where: {[str(cond) for cond in result.where_conditions]}")
        
        if result.order_by_columns:
            print(f"   Order By: {[str(col) for col in result.order_by_columns]}")
        
        if result.group_by_columns:
            print(f"   Group By: {[str(col) for col in result.group_by_columns]}")
        
        if result.limit_count:
            print(f"   Limit: {result.limit_count}")
    
    # Show parser info
    print(f"\n🔧 Parser Info:")
    info = parser.get_parser_info()
    for key, value in info.items():
        print(f"   {key}: {value}")


if __name__ == "__main__":
    main()
