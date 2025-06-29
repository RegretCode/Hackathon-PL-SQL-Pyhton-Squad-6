# SQL to PySpark Translator - Testes Unitários

## 📝 Descrição

Um tradutor que converte consultas SQL em código PySpark equivalente para realizar testes unitários abrangentes.

## 📋 Funcionalidades

- **SELECT básico** com colunas específicas
- **WHERE** com condições de filtro
- **GROUP BY** com funções de agregação
- **HAVING** para filtros pós-agregação
- **JOINs** (INNER, LEFT, RIGHT, FULL OUTER)
- **Múltiplos JOINs** em sequência
- **ORDER BY** com suporte a DESC
- **LIMIT** para limitar resultados
- **DISTINCT** para valores únicos
- **Funções de agregação**: COUNT, AVG, SUM

## 🛠 Como usar

```python
from src.translator import SQLToPySparkTranslator

translator = SQLToPySparkTranslator()

# Exemplo básico
sql = "SELECT name FROM users WHERE age > 30"
pyspark_code = translator.translate(sql)
print(pyspark_code)
```

**Saída:**
```python
df = spark.table("users")
df = df.select("name")
df = df.filter("age > 30")
```

## 💡 Exemplos de uso

### SELECT com JOIN
```sql
SELECT u.name, o.order_id 
FROM users u 
INNER JOIN orders o ON u.user_id = o.user_id
```

### GROUP BY com HAVING
```sql
SELECT department, COUNT(*) as total 
FROM employees 
GROUP BY department 
HAVING total > 5
```

## ▶️ Executar testes

```bash
# Executar todos os testes
python -m unittest discover

# Executar teste específico
python -m unittest tests.test_translator.TestSQLToPySparkTranslator.test_basic_select
```

## 📂  Estrutura do projeto

```
sql-pyspark-translator-tests/
├── src/
│   ├── __init__.py
│   └── translator.py      # Código principal
├── tests/
│   ├── __init__.py
│   └── test_translator.py # 26 testes unitários
├── requirements.txt       # Dependências
├── README.md             # Este arquivo
└── .gitignore           # Arquivos ignorados pelo Git
```

## 📦 Dependências

- Python 3.7+
- PySpark

## 📥 Instalação

```bash
pip install -r requirements.txt
```

## 🔧 Funcionalidades a serem implementadas futuramente

- UNION/UNION ALL
- Suporte a subconsultas
- Suporte a CTEs (Common Table Expressions)
- Suporte a WINDOW functions

## 🤝 Contribuição

Para adicionar novas funcionalidades:
1. Implemente a funcionalidade em `translator.py`
2. Adicione testes em `tests/test_translator.py`
3. Execute os testes para garantir que nada quebrou
4. Faça commit das mudanças