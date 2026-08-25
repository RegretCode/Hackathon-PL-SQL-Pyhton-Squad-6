"""
Script de teste para verificar a funcionalidade do tradutor SQL.
"""

import sys
import os

# Adicionar diretório src ao caminho para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_translator import SQLTranslator, translate_sql, convert_where_clause


def run_tests():
    """Executar testes básicos para verificar funcionalidade."""
    print("🧪 Executando Testes do Tradutor SQL")
    print("=" * 40)
    
    tests_passed = 0
    total_tests = 0
    
    # Teste 1: SELECT básico
    total_tests += 1
    print("\n🔍 Teste 1: Tradução de SELECT básico")
    sql = "SELECT nome, idade FROM usuarios"
    spark_sql, pyspark_code = translate_sql(sql)
    
    if pyspark_code and "usuarios_df.select(F.col('nome'), F.col('idade'))" in pyspark_code:
        print("   ✅ PASSOU: Tradução de SELECT básico funciona")
        tests_passed += 1
    else:
        print("   ❌ FALHOU: Tradução de SELECT básico")
    
    # Teste 2: Cláusula WHERE
    total_tests += 1
    print("\n🔍 Teste 2: Tradução de cláusula WHERE")
    sql = "SELECT nome FROM usuarios WHERE idade > 18"
    spark_sql, pyspark_code = translate_sql(sql)
    
    if pyspark_code and ".filter((F.col('idade') > 18))" in pyspark_code:
        print("   ✅ PASSOU: Tradução de cláusula WHERE funciona")
        tests_passed += 1
    else:
        print("   ❌ FALHOU: Tradução de cláusula WHERE")
    
    # Teste 3: ORDER BY
    total_tests += 1
    print("\n🔍 Teste 3: Tradução de ORDER BY")
    sql = "SELECT nome FROM usuarios ORDER BY idade DESC"
    spark_sql, pyspark_code = translate_sql(sql)
    
    if pyspark_code and ".orderBy(F.col('idade').desc())" in pyspark_code:
        print("   ✅ PASSOU: Tradução de ORDER BY funciona")
        tests_passed += 1
    else:
        print("   ❌ FALHOU: Tradução de ORDER BY")
    
    # Teste 4: LIMIT
    total_tests += 1
    print("\n🔍 Teste 4: Tradução de LIMIT")
    sql = "SELECT nome FROM usuarios LIMIT 10"
    spark_sql, pyspark_code = translate_sql(sql)
    
    if pyspark_code and ".limit(10)" in pyspark_code:
        print("   ✅ PASSOU: Tradução de LIMIT funciona")
        tests_passed += 1
    else:
        print("   ❌ FALHOU: Tradução de LIMIT")
    
    # Teste 5: Aliases de colunas
    total_tests += 1
    print("\n🔍 Teste 5: Tradução de aliases de colunas")
    sql = "SELECT nome, idade as user_age FROM usuarios"
    spark_sql, pyspark_code = translate_sql(sql)
    
    if pyspark_code and "F.col('idade').alias('user_age')" in pyspark_code:
        print("   ✅ PASSOU: Tradução de aliases de colunas funciona")
        tests_passed += 1
    else:
        print("   ❌ FALHOU: Tradução de aliases de colunas")
    
    # Teste 6: Classe SQLTranslator
    total_tests += 1
    print("\n🔍 Teste 6: Classe SQLTranslator")
    translator = SQLTranslator()
    result = translator.translate("SELECT * FROM test")
    
    if (isinstance(result, dict) and 
        'original_sql' in result and 
        'spark_sql' in result and 
        'pyspark_code' in result and 
        'translation_available' in result):
        print("   ✅ PASSOU: Classe SQLTranslator funciona")
        tests_passed += 1
    else:
        print("   ❌ FALHOU: Classe SQLTranslator")
    
    # Teste 7: Conversão de cláusula WHERE
    total_tests += 1
    print("\n🔍 Teste 7: Conversão de cláusula WHERE")
    where_clause = "idade > 18 AND nome = 'João'"
    result = convert_where_clause(where_clause)
    
    if "F.col('idade') > 18" in result and "F.col('nome') == 'João'" in result and " & " in result:
        print("   ✅ PASSOU: Conversão de cláusula WHERE funciona")
        tests_passed += 1
    else:
        print("   ❌ FALHOU: Conversão de cláusula WHERE")
    
    # Resumo
    print("\n" + "=" * 40)
    print(f"📊 RESUMO DOS TESTES")
    print(f"   Testes Aprovados: {tests_passed}/{total_tests}")
    print(f"   Taxa de Sucesso: {tests_passed/total_tests*100:.1f}%")
    
    if tests_passed == total_tests:
        print("   🎉 TODOS OS TESTES PASSARAM! O tradutor SQL está funcionando corretamente.")
    else:
        print(f"   ⚠️  {total_tests - tests_passed} teste(s) falharam. Por favor verifique a implementação.")
    
    return tests_passed == total_tests


if __name__ == "__main__":
    run_tests()
