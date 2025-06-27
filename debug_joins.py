"""
Debug de análise de JOIN no parser SQL
"""

import sys
import os
import re
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from translator.sql_parser import SQLParser


def debug_join_parsing():
    """Debug da análise de consultas JOIN."""
    parser = SQLParser()
    
    test_query = "SELECT u.nome, d.nome FROM usuarios u INNER JOIN departamentos d ON u.depto_id = d.id"
    
    print(f"🔍 Depurando análise de JOIN")
    print(f"Consulta: {test_query}")
    print("-" * 80)
    
    # Limpar o SQL
    cleaned_sql = parser.clean_sql(test_query)
    print(f"SQL Limpo: {cleaned_sql}")
    
    # Testar padrão regex principal
    pattern = parser.patterns['main_query']
    print(f"\nPadrão principal: {pattern}")
    
    match = re.search(pattern, cleaned_sql, re.IGNORECASE | re.DOTALL)
    
    if match:
        print(f"\n✅ Padrão principal corresponde!")
        parts = match.groupdict()
        for key, value in parts.items():
            print(f"   {key}: {value}")
        
        # Testar padrão JOIN especificamente
        joins_clause = parts.get('joins', '')
        print(f"\nCláusula JOINS: '{joins_clause}'")
        
        if joins_clause:
            join_pattern = parser.patterns['join_pattern']
            print(f"Padrão JOIN: {join_pattern}")
            
            join_matches = list(re.finditer(join_pattern, joins_clause, re.IGNORECASE))
            print(f"Correspondências JOIN encontradas: {len(join_matches)}")
            
            for i, join_match in enumerate(join_matches):
                print(f"   Correspondência {i+1}: {join_match.groups()}")
        else:
            print("❌ Nenhuma cláusula joins encontrada no padrão principal")
    else:
        print("❌ Padrão principal não correspondeu")
    
    # Agora testar com o parser
    print(f"\n🔧 Testando com o parser:")
    result = parser.parse(test_query)
    print(f"Análise bem-sucedida: {result.is_valid}")
    print(f"Erros: {result.errors}")
    print(f"Cláusulas JOIN encontradas: {len(result.join_clauses)}")
    for join in result.join_clauses:
        print(f"   - {join}")


if __name__ == "__main__":
    debug_join_parsing()
