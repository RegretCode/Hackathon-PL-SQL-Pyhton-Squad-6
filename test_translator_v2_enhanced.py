"""
Teste das Novas Funcionalidades do Tradutor SQL v2
Testa JOINs, GROUP BY, HAVING, agregações e outras funcionalidades avançadas
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_translator_v2_new import SQLTranslator


def test_enhanced_translator():
    """Testa as funcionalidades aprimoradas do tradutor."""
    print("🧪 Testando Tradutor SQL v2 Aprimorado")
    print("=" * 60)
    
    translator = SQLTranslator()
    
    # Casos de teste com funcionalidades avançadas
    test_cases = [
        {
            "name": "JOIN Simples",
            "sql": "SELECT u.nome, p.nome_produto FROM usuarios u INNER JOIN pedidos p ON u.id = p.usuario_id"
        },
        {
            "name": "GROUP BY com COUNT",
            "sql": "SELECT departamento, COUNT(*) as total FROM funcionarios GROUP BY departamento"
        },
        {
            "name": "GROUP BY com múltiplas agregações",
            "sql": "SELECT departamento, COUNT(*) as total, AVG(salario) as media_salario FROM funcionarios GROUP BY departamento"
        },
        {
            "name": "HAVING com GROUP BY",
            "sql": "SELECT departamento, COUNT(*) as total FROM funcionarios GROUP BY departamento HAVING COUNT(*) > 5"
        },
        {
            "name": "LEFT JOIN com WHERE",
            "sql": "SELECT u.nome, p.valor FROM usuarios u LEFT JOIN pedidos p ON u.id = p.usuario_id WHERE u.idade > 25"
        },
        {
            "name": "Agregação sem GROUP BY",
            "sql": "SELECT COUNT(*), SUM(salario), AVG(idade) FROM funcionarios"
        },
        {
            "name": "WHERE com IS NULL",
            "sql": "SELECT nome FROM usuarios WHERE email IS NULL"
        },
        {
            "name": "WHERE com IN",
            "sql": "SELECT nome FROM funcionarios WHERE departamento IN ('TI', 'RH', 'Vendas')"
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. {test_case['name']}")
        print(f"   SQL: {test_case['sql']}")
        
        try:
            result = translator.translate(test_case['sql'])
            
            print(f"   ✅ Válido: {result['parsed_sql'].is_valid}")
            if result['parsed_sql'].errors:
                print(f"   ⚠️  Erros: {result['parsed_sql'].errors}")
            
            print(f"   🔥 Spark SQL: {result['spark_sql']}")
            
            if result['translation_available']:
                print(f"   🐍 PySpark:")
                pyspark_lines = result['pyspark_code'].split('\n')
                for line in pyspark_lines:
                    if line.strip():
                        print(f"      {line}")
                
                # Mostrar informações adicionais
                features = []
                if result.get('has_joins'):
                    features.append("JOINs")
                if result.get('has_group_by'):
                    features.append("GROUP BY")
                if result.get('has_aggregates'):
                    features.append("Agregações")
                if result.get('has_having'):
                    features.append("HAVING")
                
                if features:
                    print(f"   📊 Funcionalidades: {', '.join(features)}")
            else:
                print(f"   ❌ Tradução PySpark não disponível")
                
        except Exception as e:
            print(f"   💥 Erro: {e}")
    
    print(f"\n📊 Total de traduções realizadas: {translator.get_translation_count()}")
    
    # Testar informações do parser
    parser_info = translator.get_parser_info()
    print(f"\n🔧 Capacidades do Parser:")
    print(f"   Cláusulas SQL: {', '.join(parser_info.get('supported_clauses', []))}")
    print(f"   Operadores: {', '.join(parser_info.get('supported_operators', []))}")
    print(f"   Agregações: {', '.join(parser_info.get('supported_aggregates', []))}")


def test_parse_only():
    """Testa a funcionalidade de análise apenas."""
    print("\n" + "=" * 60)
    print("🔍 Teste de Análise SQL (sem tradução)")
    print("=" * 60)
    
    translator = SQLTranslator()
    
    sql = "SELECT d.nome, COUNT(f.id) as total_funcionarios FROM departamentos d LEFT JOIN funcionarios f ON d.id = f.departamento_id GROUP BY d.nome HAVING COUNT(f.id) > 10 ORDER BY total_funcionarios DESC"
    
    print(f"SQL: {sql}")
    
    result = translator.parse_only(sql)
    
    print(f"\n📋 Resultado da Análise:")
    print(f"   Válido: {result['is_valid']}")
    print(f"   Tabela: {result['table']}")
    print(f"   Colunas: {result['columns']}")
    print(f"   JOINs: {result['joins']}")
    print(f"   WHERE: {result['where_conditions']}")
    print(f"   GROUP BY: {result['group_by']}")
    print(f"   HAVING: {result['having_conditions']}")
    print(f"   ORDER BY: {result['order_by']}")
    print(f"   LIMIT: {result['limit']}")
    print(f"   Tem Agregações: {result['has_aggregates']}")
    
    if result['errors']:
        print(f"   Erros: {result['errors']}")


if __name__ == "__main__":
    test_enhanced_translator()
    test_parse_only()
    print("\n🎉 Testes do Tradutor v2 Aprimorado concluídos!")
