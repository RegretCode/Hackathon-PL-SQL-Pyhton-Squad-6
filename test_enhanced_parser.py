"""
Suíte de Testes do Analisador SQL Aprimorado

Este script testa o analisador SQL com vários recursos SQL incluindo:
- JOINs INNER, LEFT, RIGHT, FULL
- Funções de agregação (COUNT, SUM, AVG, MAX, MIN)
- Cláusulas GROUP BY e HAVING
- Condições WHERE complexas
- ORDER BY com múltiplas colunas
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from translator.sql_parser import SQLParser, ParsedSQL


class ParserTestSuite:
    """Suíte de testes para o analisador SQL aprimorado."""
    
    def __init__(self):
        self.parser = SQLParser()
        self.test_results = []
    
    def run_test(self, query_name: str, query: str, expected_features: dict = None):
        """Executar um único teste e registrar os resultados."""
        print(f"\n🔍 Testando: {query_name}")
        print(f"Consulta: {query}")
        print("-" * 80)
        
        result = self.parser.parse(query)
        
        test_passed = True
        issues = []
        
        if not result.is_valid:
            print(f"❌ Análise falhou: {', '.join(result.errors)}")
            test_passed = False
        else:
            print("✅ Análise bem-sucedida!")
            
            # Exibir componentes analisados
            print(f"📊 Componentes Analisados:")
            print(f"   Tabela FROM: {result.from_table}")
            print(f"   Colunas SELECT: {len(result.select_columns)}")
            for col in result.select_columns:
                if col.is_aggregate:
                    print(f"      - {col.aggregate_function}({col.name})" + (f" AS {col.alias}" if col.alias else ""))
                else:
                    print(f"      - {col.name}" + (f" AS {col.alias}" if col.alias else ""))
            
            if result.join_clauses:
                print(f"   Cláusulas JOIN: {len(result.join_clauses)}")
                for join in result.join_clauses:
                    print(f"      - {join.join_type} {join.table} ON {join.condition}")
            
            if result.where_conditions:
                print(f"   Condições WHERE: {len(result.where_conditions)}")
                for cond in result.where_conditions:
                    print(f"      - {cond.column} {cond.operator} {cond.value}")
            
            if result.group_by_columns:
                print(f"   GROUP BY: {[col.column for col in result.group_by_columns]}")
            
            if result.having_conditions:
                print(f"   HAVING: {[str(cond) for cond in result.having_conditions]}")
            
            if result.order_by_columns:
                print(f"   ORDER BY: {[f'{col.column} {col.direction}' for col in result.order_by_columns]}")
            
            if result.limit_count:
                print(f"   LIMIT: {result.limit_count}")
            
            # Verificar recursos esperados se fornecidos
            if expected_features:
                if 'joins' in expected_features and len(result.join_clauses) != expected_features['joins']:
                    issues.append(f"Esperado {expected_features['joins']} joins, obtido {len(result.join_clauses)}")
                
                if 'aggregates' in expected_features:
                    actual_aggregates = sum(1 for col in result.select_columns if col.is_aggregate)
                    if actual_aggregates != expected_features['aggregates']:
                        issues.append(f"Esperado {expected_features['aggregates']} agregações, obtido {actual_aggregates}")
                
                if 'group_by' in expected_features and len(result.group_by_columns) != expected_features['group_by']:
                    issues.append(f"Esperado {expected_features['group_by']} colunas GROUP BY, obtido {len(result.group_by_columns)}")
        
        if issues:
            print(f"⚠️  Problemas encontrados: {', '.join(issues)}")
            test_passed = False
        
        self.test_results.append({
            'name': query_name,
            'query': query,
            'passed': test_passed,
            'issues': issues,
            'valid': result.is_valid
        })
        
        return result
    
    def test_basic_queries(self):
        """Testar consultas SQL básicas."""
        print("\n" + "=" * 80)
        print("🎯 TESTANDO CONSULTAS BÁSICAS")
        print("=" * 80)
        
        self.run_test(
            "SELECT simples",
            "SELECT nome, idade FROM usuarios"
        )
        
        self.run_test(
            "SELECT com WHERE",
            "SELECT nome, idade FROM usuarios WHERE idade > 18"
        )
        
        self.run_test(
            "SELECT com ORDER BY",
            "SELECT nome, salario FROM funcionarios ORDER BY salario DESC"
        )
        
        self.run_test(
            "SELECT com LIMIT",
            "SELECT * FROM produtos LIMIT 10"
        )
    
    def test_join_queries(self):
        """Testar consultas JOIN."""
        print("\n" + "=" * 80)
        print("🔗 TESTANDO CONSULTAS JOIN")
        print("=" * 80)
        
        self.run_test(
            "INNER JOIN",
            "SELECT u.nome, d.nome FROM usuarios u INNER JOIN departamentos d ON u.depto_id = d.id",
            {'joins': 1}
        )
        
        self.run_test(
            "LEFT JOIN",
            "SELECT c.nome, p.titulo FROM clientes c LEFT JOIN pedidos p ON c.id = p.cliente_id",
            {'joins': 1}
        )
        
        self.run_test(
            "RIGHT JOIN",
            "SELECT e.nome, p.nome FROM empregados e RIGHT JOIN projetos p ON e.projeto_id = p.id",
            {'joins': 1}
        )
        
        self.run_test(
            "Múltiplos JOINs",
            """SELECT u.nome, d.nome, c.nome 
            FROM usuarios u 
            INNER JOIN departamentos d ON u.depto_id = d.id 
            LEFT JOIN cidades c ON u.cidade_id = c.id""",
            {'joins': 2}
        )
    
    def test_aggregate_queries(self):
        """Testar consultas com funções de agregação."""
        print("\n" + "=" * 80)
        print("📊 TESTANDO CONSULTAS COM AGREGAÇÃO")
        print("=" * 80)
        
        self.run_test(
            "Função COUNT",
            "SELECT COUNT(*) FROM usuarios",
            {'aggregates': 1}
        )
        
        self.run_test(
            "COUNT com alias",
            "SELECT COUNT(*) AS total_usuarios FROM usuarios",
            {'aggregates': 1}
        )
        
        self.run_test(
            "Função SUM",
            "SELECT SUM(salario) AS total_salarios FROM funcionarios",
            {'aggregates': 1}
        )
        
        self.run_test(
            "Função AVG",
            "SELECT AVG(idade) AS idade_media FROM usuarios",
            {'aggregates': 1}
        )
        
        self.run_test(
            "MAX e MIN",
            "SELECT MAX(salario) AS maior_salario, MIN(salario) AS menor_salario FROM funcionarios",
            {'aggregates': 2}
        )
        
        self.run_test(
            "Múltiplas agregações com GROUP BY",
            "SELECT departamento, COUNT(*) AS total, AVG(salario) AS salario_medio FROM funcionarios GROUP BY departamento",
            {'aggregates': 2, 'group_by': 1}
        )
    
    def test_group_by_having(self):
        """Testar cláusulas GROUP BY e HAVING."""
        print("\n" + "=" * 80)
        print("📈 TESTANDO GROUP BY E HAVING")
        print("=" * 80)
        
        self.run_test(
            "GROUP BY simples",
            "SELECT departamento, COUNT(*) FROM funcionarios GROUP BY departamento",
            {'group_by': 1, 'aggregates': 1}
        )
        
        self.run_test(
            "GROUP BY com HAVING",
            "SELECT departamento, COUNT(*) AS total FROM funcionarios GROUP BY departamento HAVING COUNT(*) > 5",
            {'group_by': 1, 'aggregates': 1}
        )
        
        self.run_test(
            "GROUP BY com múltiplas colunas",
            "SELECT departamento, cidade, COUNT(*) FROM funcionarios GROUP BY departamento, cidade",
            {'group_by': 2, 'aggregates': 1}
        )
    
    def test_complex_queries(self):
        """Testar consultas complexas com múltiplos recursos."""
        print("\n" + "=" * 80)
        print("🔥 TESTANDO CONSULTAS COMPLEXAS")
        print("=" * 80)
        
        self.run_test(
            "Consulta complexa com tudo",
            """SELECT d.nome, COUNT(f.id) AS total_funcionarios, AVG(f.salario) AS salario_medio
            FROM departamentos d
            LEFT JOIN funcionarios f ON d.id = f.depto_id
            WHERE f.ativo = true
            GROUP BY d.nome
            HAVING COUNT(f.id) > 2
            ORDER BY salario_medio DESC
            LIMIT 5""",
            {'joins': 1, 'aggregates': 2, 'group_by': 1}
        )
        
        self.run_test(
            "Consulta com múltiplas condições WHERE",
            "SELECT nome, salario FROM funcionarios WHERE salario > 50000 AND departamento = 'TI' AND ativo = true"
        )
        
        self.run_test(
            "Consulta com cláusula IN",
            "SELECT nome FROM usuarios WHERE id IN (1, 2, 3, 4, 5)"
        )
        
        self.run_test(
            "Consulta com cláusula LIKE",
            "SELECT nome FROM produtos WHERE nome LIKE '%smartphone%'"
        )
    
    def run_all_tests(self):
        """Executar todas as suítes de teste."""
        print("🧪 Suíte de Testes do Analisador SQL Aprimorado")
        print("=" * 80)
        
        self.test_basic_queries()
        self.test_join_queries()
        self.test_aggregate_queries()
        self.test_group_by_having()
        self.test_complex_queries()
        
        # Resumo
        print("\n" + "=" * 80)
        print("📋 RESUMO DOS TESTES")
        print("=" * 80)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['passed'])
        failed_tests = total_tests - passed_tests
        
        print(f"Total de testes: {total_tests}")
        print(f"✅ Aprovados: {passed_tests}")
        print(f"❌ Falharam: {failed_tests}")
        print(f"Taxa de sucesso: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            print(f"\n🔍 Testes que falharam:")
            for result in self.test_results:
                if not result['passed']:
                    print(f"   - {result['name']}: {', '.join(result['issues']) if result['issues'] else 'Erro de análise'}")
        
        return passed_tests, failed_tests


def main():
    """Executar a suíte de testes do analisador aprimorado."""
    test_suite = ParserTestSuite()
    passed, failed = test_suite.run_all_tests()
    
    if failed == 0:
        print(f"\n🎉 Todos os testes foram aprovados! O analisador está funcionando corretamente.")
    else:
        print(f"\n⚠️  {failed} teste(s) falharam. Revise os problemas acima.")
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
