"""
Debug do parser para identificar problemas.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sql_parser.parser import SQLParser

def debug_parser():
    """Debug detalhado do parser."""
    sql_query = "SELECT id, name FROM users WHERE age > 18"
    
    print("Debugging parser:")
    print(f"SQL: {sql_query}")
    
    try:
        sql_parser = SQLParser()
        ast = sql_parser.parse(sql_query)
        
        def print_node(node, indent=0):
            prefix = "  " * indent
            print(f"{prefix}{node.node_type}: '{node.value}' ({len(node.children)} children)")
            if node.attributes:
                print(f"{prefix}  attributes: {node.attributes}")
            for child in node.children:
                print_node(child, indent + 1)
        
        print("\nAST Structure:")
        print_node(ast)
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_parser()