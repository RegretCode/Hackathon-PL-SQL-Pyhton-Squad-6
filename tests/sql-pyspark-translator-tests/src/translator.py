import re
from pyspark.sql.functions import *

class SQLToPySparkTranslator:
    def translate(self, sql):
        sql = sql.strip()
        components = self._parse_sql(sql)
        code_lines = []

        # Table references - Check for multiple JOINs first
        if 'joins' in components and len(components['joins']) > 1:
            # Multiple JOINs case
            # Extract base table from FROM clause
            base_table_parts = components['from'].split()
            base_table = base_table_parts[0]
            base_alias = base_table_parts[1] if len(base_table_parts) > 1 else base_table
            
            # Create mapping from aliases to table names
            alias_to_table = {base_alias: base_table}
            
            # Generate code for base table
            code_lines.append(f'df_{base_table} = spark.table("{base_table}")')
            
            # Generate code for all JOIN tables
            tables_added = set([base_table])
            for join in components['joins']:
                join_table = join['table']
                if join_table not in tables_added:
                    code_lines.append(f'df_{join_table} = spark.table("{join_table}")')
                    tables_added.add(join_table)
                
                # Add alias to mapping
                if join['alias']:
                    alias_to_table[join['alias']] = join_table
            
            # Start with base table
            code_lines.append(f'df = df_{base_table}')
            
            # Process each JOIN in sequence
            for join in components['joins']:
                left_on = join['on'][0].split(".")
                right_on = join['on'][1].split(".")
                
                if len(left_on) < 2 or len(right_on) < 2:
                    raise ValueError("Invalid JOIN condition. Expected format 'alias.column = alias.column'")
                
                # Determine which table is referenced on each side of the JOIN
                left_alias = left_on[0]
                right_alias = right_on[0]
                left_table = alias_to_table.get(left_alias, left_alias)
                right_table = alias_to_table.get(right_alias, right_alias)
                
                # Generate code for the JOIN
                code_lines.append(
                    f'df = df.join(df_{right_table}, '
                    f'df_{left_table}["{left_on[1]}"] == df_{right_table}["{right_on[1]}"]'
                    f', "{join["type"]}")'
                )
        elif 'join' in components:
            # Single JOIN case (for backward compatibility)
            left_table = components['from'].split()[0]
            right_table = components['join']['table']
            
            # Use full table names for dataframes instead of aliases
            code_lines.append(f'df_{left_table} = spark.table("{left_table}")')
            code_lines.append(f'df_{right_table} = spark.table("{right_table}")')

            left_on = components['join']['on'][0].split(".")
            right_on = components['join']['on'][1].split(".")

            if len(left_on) < 2 or len(right_on) < 2:
                raise ValueError("Invalid JOIN condition. Expected format 'alias.column = alias.column'")

            # Get join type (default to inner)
            join_type = "inner"
            
            # Check for LEFT JOIN
            if re.search(r'left\s+join', sql, re.IGNORECASE):
                join_type = "left"
            # Check for RIGHT JOIN
            elif re.search(r'right\s+join', sql, re.IGNORECASE):
                join_type = "right"
            # Check for FULL OUTER JOIN
            elif re.search(r'full\s+outer\s+join|full\s+join', sql, re.IGNORECASE):
                join_type = "full"
                
            code_lines.append(
                f'df = df_{left_table}.join(df_{right_table}, '
                f'df_{left_table}["{left_on[1]}"] == df_{right_table}["{right_on[1]}"]'
                f', "{join_type}")'
            )
        else:
            # No join case
            table_parts = components["from"].split()
            table_name = table_parts[0]
            code_lines.append(f'df = spark.table("{table_name}")')

        # Select clause for non-join, non-group by queries
        if 'select' in components and not components.get('group_by') and not (components.get('join') or components.get('joins')):
            select_cols = [f'"{col}"' for col in components['select']]
            select_stmt = f'df = df.select({", ".join(select_cols)})'
            if components.get('distinct', False):
                select_stmt += '.distinct()'
            code_lines.append(select_stmt)
        # Select clause for join queries without group by (must come after join but before where)
        elif 'select' in components and (components.get('join') or components.get('joins')) and not components.get('group_by'):
            code_lines.append(f'df = df.select({", ".join([f"\"{col.split('.')[-1]}\"" for col in components["select"]])})')
        
        # Where clause (after select for join queries)
        if 'where' in components:
            # Remove table aliases from the where condition if present
            where_condition = components["where"]
            # Replace patterns like "alias.column" with just "column"
            where_condition = re.sub(r'\w+\.(\w+)', r'\1', where_condition)
            code_lines.append(f'df = df.filter("{where_condition}")')
        
        # Group by and aggregation
        if 'group_by' in components:
            group_cols = [f'"{col.split(".")[-1]}"' for col in components['group_by']]
            code_lines.append(f'df = df.groupBy({", ".join(group_cols)})')
            
            if 'select' in components:
                agg_cols = []
                for col in components['select']:
                    if col in components['group_by'] or col.split(".")[-1] in [g.split(".")[-1] for g in components['group_by']]:
                        continue
                    
                    # Handle COUNT(*), AVG(col), SUM(col), etc.
                    agg_match = re.match(r'(\w+)\((.*?)\)(?: as (\w+))?', col, re.IGNORECASE)
                    if agg_match:
                        agg_func = agg_match.group(1).lower()
                        agg_col = agg_match.group(2)
                        alias = agg_match.group(3) if agg_match.group(3) else f"{agg_func}_{agg_col}"
                        agg_cols.append(f'{agg_func}("{agg_col}").alias("{alias}")')
                
                if agg_cols:
                    code_lines.append(f'df = df.agg({", ".join(agg_cols)})')
        
        # Having clause (must come after group by)
        if 'having' in components:
            code_lines.append(f'df = df.filter("{components["having"]}")')
        
        # Order by clause
        if 'order_by' in components:
            order_by = components["order_by"]
            # Remove table aliases from the order by condition if present
            order_by = re.sub(r'\w+\.(\w+)', r'\1', order_by)
            # Handle DESC after the column name
            if " DESC" in order_by.upper():
                order_by = order_by.replace(" DESC", "").replace(" desc", "")
                code_lines.append(f'df = df.orderBy("{order_by}")')
            else:
                code_lines.append(f'df = df.orderBy("{order_by}")')
        
        # Limit clause
        if 'limit' in components:
            code_lines.append(f'df = df.limit({components["limit"]})')

        return "\n".join(code_lines)
    
    def _parse_sql(self, sql):
        components = {}

        select_match = re.search(r'select (.*?) from', sql, re.IGNORECASE | re.DOTALL)
        
        # Novo padrão para capturar todos os JOINs
        join_pattern = r'(?P<join_type>inner|left|right|full\s+outer|full)?\s*join\s+(?P<table>\w+)(?:\s+(?P<alias>\w+))?\s+on\s+(?P<left_col>[\w.]+)\s*=\s*(?P<right_col>[\w.]+)'
        join_matches = list(re.finditer(join_pattern, sql, re.IGNORECASE))
        
        from_match = re.search(r'from\s+(\w+)(?:\s+(\w+))?(?:\s+(?:(?:inner|left|right|full\s+outer|full)\s+)?join|\s+where|\s+group\s+by|\s+having|\s+order\s+by|\s+limit|$)', sql, re.IGNORECASE)
        where_match = re.search(r'where (.*?)( group by| having| order by| limit|$)', sql, re.IGNORECASE | re.DOTALL)
        group_by_match = re.search(r'group by (.*?)( having| order by| limit|$)', sql, re.IGNORECASE | re.DOTALL)
        having_match = re.search(r'having (.*?)( order by| limit|$)', sql, re.IGNORECASE | re.DOTALL)
        order_by_match = re.search(r'order by (.*?)( limit|$)', sql, re.IGNORECASE)
        limit_match = re.search(r'limit (\d+)', sql, re.IGNORECASE)

        # Processamento do SELECT
        if select_match:
            select_part = select_match.group(1)
            if select_part.upper().startswith('DISTINCT'):
                components['distinct'] = True
                select_part = select_part[8:].strip()
            components['select'] = [s.strip() for s in select_part.split(',')]

        # Processamento do FROM
        if from_match:
            table_name = from_match.group(1)
            alias = from_match.group(2)
            components['from'] = f"{table_name} {alias}" if alias else table_name

        # Processamento de múltiplos JOINs
        if join_matches:
            components['joins'] = []
            for match in join_matches:
                join_type = match.group('join_type') or 'inner'
                join_type = join_type.lower()
                
                # Normalizar o tipo de JOIN
                if join_type.startswith('full'):
                    join_type = 'full'
                
                components['joins'].append({
                    'table': match.group('table'),
                    'alias': match.group('alias'),
                    'on': [match.group('left_col').strip(), match.group('right_col').strip()],
                    'type': join_type
                })
            
            # Para compatibilidade com o código existente, mantenha o primeiro JOIN também no formato antigo
            if len(join_matches) > 0:
                first_match = join_matches[0]
                components['join'] = {
                    'table': first_match.group('table'),
                    'alias': first_match.group('alias'),
                    'on': [first_match.group('left_col').strip(), first_match.group('right_col').strip()]
                }
        
        # Processamento do WHERE
        if where_match:
            components['where'] = where_match.group(1).strip()
        
        # Processamento do GROUP BY
        if group_by_match:
            components['group_by'] = [col.strip() for col in group_by_match.group(1).split(',')]
        
        # Processamento do HAVING
        if having_match:
            components['having'] = having_match.group(1).strip()
        
        # Processamento do ORDER BY
        if order_by_match:
            components['order_by'] = order_by_match.group(1).strip()
        
        # Processamento do LIMIT
        if limit_match:
            components['limit'] = int(limit_match.group(1))
        
        return components