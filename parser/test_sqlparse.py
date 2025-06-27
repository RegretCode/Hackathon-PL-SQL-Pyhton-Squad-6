"""
Teste direto do sqlparse para entender como funciona.
"""

import sqlparse

def test_sqlparse():
    sql = "SELECT id, name FROM users WHERE age > 18"
    
    print(f"SQL: {sql}")
    print("\nParsing with sqlparse:")
    
    parsed = sqlparse.parse(sql)
    print(f"Number of statements: {len(parsed)}")
    
    if parsed:
        statement = parsed[0]
        print(f"Statement type: {statement.get_type()}")
        print(f"Number of tokens: {len(statement.tokens)}")
        
        print("\nTokens:")
        for i, token in enumerate(statement.tokens):
            print(f"  {i}: {repr(token)} (type: {token.ttype}, is_keyword: {getattr(token, 'is_keyword', False)})")
            if hasattr(token, 'normalized'):
                print(f"      normalized: {token.normalized}")

if __name__ == "__main__":
    test_sqlparse()