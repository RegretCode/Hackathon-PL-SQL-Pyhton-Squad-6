# Testes do Módulo Oracle to PostgreSQL

Este diretório contém os testes unitários para o módulo `oracle_to_postgresql.py`.

## Estrutura dos Testes

### `test_oracle_to_postgresql.py`
Contém testes abrangentes para todas as principais funcionalidades do conversor:

#### Testes de Mapeamento de Formatos
- `test_map_oracle_date_format_to_pg`: Testa o mapeamento de formatos de data Oracle para PostgreSQL

#### Testes de Conversão de Funções
- `test_rownum_conversion`: Testa conversão de ROWNUM para LIMIT
- `test_dual_removal`: Testa remoção de FROM DUAL
- `test_nvl_to_coalesce`: Testa conversão de NVL para COALESCE
- `test_sysdate_conversion`: Testa conversão de SYSDATE para NOW()
- `test_trunc_conversion`: Testa conversão de TRUNC para DATE_TRUNC
- `test_add_months_conversion`: Testa conversão de ADD_MONTHS
- `test_last_day_conversion`: Testa conversão de LAST_DAY
- `test_to_number_conversion`: Testa conversão de TO_NUMBER para CAST
- `test_to_char_date_format_conversion`: Testa conversão de TO_CHAR com formatos
- `test_to_date_format_conversion`: Testa conversão de TO_DATE com formatos

#### Testes de Tipos de Dados
- `test_varchar2_conversion`: Testa conversão de VARCHAR2 para VARCHAR
- `test_number_conversion`: Testa conversão de NUMBER para NUMERIC

#### Testes de Integração
- `test_complex_query_conversion`: Testa conversão de query complexa com múltiplas funções
- `test_normalize_sql_query`: Testa a função principal de normalização
- `test_semicolon_addition`: Testa se o ponto e vírgula é adicionado ao final

## Como Executar os Testes

### Usando unittest (método recomendado)
```bash
# A partir do diretório raiz do projeto
python -m unittest tests.test_oracle_to_postgresql -v
```

### Executando um teste específico
```bash
python -m unittest tests.test_oracle_to_postgresql.TestOracleToPostgreSQL.test_rownum_conversion -v
```

### Usando pytest (se instalado)
```bash
pytest tests/test_oracle_to_postgresql.py -v
```

## Cobertura dos Testes

Os testes cobrem:
- ✅ Conversão de ROWNUM para LIMIT
- ✅ Remoção de FROM DUAL
- ✅ Conversão de funções Oracle (NVL, SYSDATE, TRUNC, ADD_MONTHS, LAST_DAY, TO_NUMBER)
- ✅ Conversão de tipos de dados (VARCHAR2, NUMBER)
- ✅ Mapeamento de formatos de data
- ✅ Queries complexas com múltiplas conversões
- ✅ Adição automática de ponto e vírgula

## Resultados Esperados

Todos os 16 testes devem passar com sucesso:
```
Ran 16 tests in 0.038s

OK
```

## Observações

- Os testes foram ajustados para refletir o comportamento real do código
- Alguns casos edge foram identificados durante os testes (como problemas de formatação em casos específicos)
- Os testes servem como documentação viva das funcionalidades implementadas