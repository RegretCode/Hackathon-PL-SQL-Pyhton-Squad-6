"""
Teste simples para verificar se o parser funciona.
"""

import sys
import os

# Adiciona o diretório pai ao path para permitir imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sql_parser.parser import SQLParser

def test_simple_query():
    """Testa uma query SQL simples."""
    sql_query = "SELECT id, name FROM users WHERE age > 18"
    
    print("Testando query SQL simples:")
    print(f"SQL: {sql_query}")
    
    try:
        # Parse the SQL query
        print("\nParsing SQL query...")
        sql_parser = SQLParser()
        ast = sql_parser.parse(sql_query)
        print("SQL query parsed successfully!")
        
        # Print AST structure
        print(f"\nAST root type: {ast.node_type}")
        print(f"Number of children: {len(ast.children)}")
        
        if ast.children:
            statement = ast.children[0]
            print(f"First statement type: {statement.node_type}")
            print(f"Statement children: {len(statement.children)}")
            
            for child in statement.children:
                print(f"  - {child.node_type}: {len(child.children) if child.children else 0} children")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_simple_query()
    if success:
        print("\n✅ Teste passou! O parser está funcionando.")
    else:
        print("\n❌ Teste falhou.")