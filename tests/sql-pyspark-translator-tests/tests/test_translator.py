import unittest
import sys
import os
# Adiciona a pasta pai ao sys.path para encontrar 'src/translator.py'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.translator import SQLToPySparkTranslator

class TestSQLToPySparkTranslator(unittest.TestCase):

    def setUp(self):
        self.translator = SQLToPySparkTranslator()

    def test_basic_select(self):
        sql = "SELECT name, age FROM users"
        expected = (
            'df = spark.table("users")\n'
            'df = df.select("name", "age")'
        )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_where_clause(self):
        sql = "SELECT name FROM users WHERE age > 30"
        expected = (
            'df = spark.table("users")\n'
            'df = df.select("name")\n'
            'df = df.filter("age > 30")'
        )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_group_by_count(self):
        sql = "SELECT department, COUNT(*) as total FROM employees GROUP BY department"
        expected = (
            'df = spark.table("employees")\n'
            'df = df.groupBy("department")\n'
            'df = df.agg(count("*").alias("total"))'
        )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_group_by_avg(self):
        sql = "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department"
        expected = (
            'df = spark.table("employees")\n'
            'df = df.groupBy("department")\n'
            'df = df.agg(avg("salary").alias("avg_salary"))'
        )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_order_by(self):
        sql = "SELECT name FROM users ORDER BY age DESC"
        expected = (
            'df = spark.table("users")\n'
            'df = df.select("name")\n'
            'df = df.orderBy("age")'
        )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_limit_clause(self):
        sql = "SELECT name FROM users LIMIT 10"
        expected = (
        'df = spark.table("users")\n'
        'df = df.select("name")\n'
        'df = df.limit(10)'
        )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_group_by_having(self): # NOVO TESTE
        sql = "SELECT department, COUNT(*) as total FROM employees GROUP BY department HAVING total > 5"
        expected = (
        'df = spark.table("employees")\n'
        'df = df.groupBy("department")\n'
        'df = df.agg(count("*").alias("total"))\n'
        'df = df.filter("total > 5")'
        )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_group_by_having_with_and(self):
        sql = "SELECT department, COUNT(*) as total FROM employees GROUP BY department HAVING COUNT(*) > 5 AND department != 'HR'"
        expected = (
        'df = spark.table("employees")\n'
        'df = df.groupBy("department")\n'
        'df = df.agg(count("*").alias("total"))\n'
        'df = df.filter("COUNT(*) > 5 AND department != \'HR\'")'
    )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_group_by_having_with_or(self):
        sql = "SELECT department, SUM(sales) as total_sales FROM employees GROUP BY department HAVING SUM(sales) > 1000 OR department = 'Marketing'"
        expected = (
        'df = spark.table("employees")\n'
        'df = df.groupBy("department")\n'
        'df = df.agg(sum("sales").alias("total_sales"))\n'
        'df = df.filter("SUM(sales) > 1000 OR department = \'Marketing\'")'
    )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_group_by_having_with_alias_in_condition(self):
        sql = "SELECT department, COUNT(*) as total FROM employees GROUP BY department HAVING total > 10"
        expected = (
        'df = spark.table("employees")\n'
        'df = df.groupBy("department")\n'
        'df = df.agg(count("*").alias("total"))\n'
        'df = df.filter("total > 10")'
    )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_group_by_having_complex_condition(self):
        sql = ("SELECT department, AVG(salary) as avg_salary FROM employees "
           "GROUP BY department HAVING AVG(salary) BETWEEN 50000 AND 100000")
        expected = (
        'df = spark.table("employees")\n'
        'df = df.groupBy("department")\n'
        'df = df.agg(avg("salary").alias("avg_salary"))\n'
        'df = df.filter("AVG(salary) BETWEEN 50000 AND 100000")'
    )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_inner_join_basic(self):
        sql = (
            "SELECT u.name, o.order_id FROM users u "
            "INNER JOIN orders o ON u.user_id = o.user_id"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "inner")\n'
        'df = df.select("name", "order_id")'
    )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_inner_join_with_where(self):
        sql = (
            "SELECT u.name, o.order_id FROM users u "
            "INNER JOIN orders o ON u.user_id = o.user_id "
            "WHERE o.price > 100"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "inner")\n'
        'df = df.select("name", "order_id")\n'
        'df = df.filter("price > 100")'
    )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_left_join(self):
        sql = (
            "SELECT u.name, o.order_id FROM users u "
            "LEFT JOIN orders o ON u.user_id = o.user_id"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "left")\n'
        'df = df.select("name", "order_id")'
    )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_join_with_where_and_order_by(self):
        sql = (
            "SELECT u.name, o.order_id FROM users u "
            "LEFT JOIN orders o ON u.user_id = o.user_id "
            "WHERE o.status = 'shipped' "
            "ORDER BY u.name"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "left")\n'
        'df = df.select("name", "order_id")\n'
        'df = df.filter("status = \'shipped\'")\n'
        'df = df.orderBy("name")'
    )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_join_with_group_by_and_having(self):
        sql = (
            "SELECT u.country, COUNT(*) as user_count FROM users u "
            "LEFT JOIN orders o ON u.user_id = o.user_id "
            "GROUP BY u.country "
            "HAVING COUNT(*) > 10 "
            "ORDER BY user_count DESC"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "left")\n'
        'df = df.groupBy("country")\n'
        'df = df.agg(count("*").alias("user_count"))\n'
        'df = df.filter("COUNT(*) > 10")\n'
        'df = df.orderBy("user_count")'
    )
        self.assertEqual(self.translator.translate(sql), expected)

    def test_distinct(self):
        sql = "SELECT DISTINCT department FROM employees"
        expected = (
        'df = spark.table("employees")\n'
        'df = df.select("department").distinct()'
    )
        self.assertEqual(self.translator.translate(sql), expected)
        
    def test_right_join(self):
        sql = (
            "SELECT u.name, o.order_id FROM users u "
            "RIGHT JOIN orders o ON u.user_id = o.user_id"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "right")\n'
        'df = df.select("name", "order_id")'
    )
        self.assertEqual(self.translator.translate(sql), expected)
        
    def test_right_join_with_where_and_order_by(self):
        sql = (
            "SELECT u.name, o.order_id FROM users u "
            "RIGHT JOIN orders o ON u.user_id = o.user_id "
            "WHERE o.status = 'processing' "
            "ORDER BY o.order_id"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "right")\n'
        'df = df.select("name", "order_id")\n'
        'df = df.filter("status = \'processing\'")\n'
        'df = df.orderBy("order_id")'
    )
        self.assertEqual(self.translator.translate(sql), expected)
        
    def test_right_join_with_group_by_and_having(self):
        sql = (
            "SELECT o.product_category, COUNT(*) as order_count FROM users u "
            "RIGHT JOIN orders o ON u.user_id = o.user_id "
            "GROUP BY o.product_category "
            "HAVING COUNT(*) > 5 "
            "ORDER BY order_count DESC"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "right")\n'
        'df = df.groupBy("product_category")\n'
        'df = df.agg(count("*").alias("order_count"))\n'
        'df = df.filter("COUNT(*) > 5")\n'
        'df = df.orderBy("order_count")'
    )
        self.assertEqual(self.translator.translate(sql), expected)
        
    def test_full_outer_join(self):
        sql = (
            "SELECT u.name, o.order_id FROM users u "
            "FULL OUTER JOIN orders o ON u.user_id = o.user_id"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "full")\n'
        'df = df.select("name", "order_id")'
    )
        self.assertEqual(self.translator.translate(sql), expected)
        
    def test_full_outer_join_with_where_and_order_by(self):
        sql = (
            "SELECT u.name, o.order_id FROM users u "
            "FULL OUTER JOIN orders o ON u.user_id = o.user_id "
            "WHERE o.status = 'pending' OR u.name IS NOT NULL "
            "ORDER BY o.order_id"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "full")\n'
        'df = df.select("name", "order_id")\n'
        'df = df.filter("status = \'pending\' OR name IS NOT NULL")\n'
        'df = df.orderBy("order_id")'
    )
        self.assertEqual(self.translator.translate(sql), expected)
        
    def test_full_outer_join_with_group_by_and_having(self):
        sql = (
            "SELECT o.product_category, COUNT(*) as order_count FROM users u "
            "FULL OUTER JOIN orders o ON u.user_id = o.user_id "
            "GROUP BY o.product_category "
            "HAVING COUNT(*) > 5 "
            "ORDER BY order_count DESC"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df = df_users.join(df_orders, df_users["user_id"] == df_orders["user_id"], "full")\n'
        'df = df.groupBy("product_category")\n'
        'df = df.agg(count("*").alias("order_count"))\n'
        'df = df.filter("COUNT(*) > 5")\n'
        'df = df.orderBy("order_count")'
    )
        self.assertEqual(self.translator.translate(sql), expected)
        
    def test_multiple_joins(self):
        sql = (
            "SELECT u.name, o.order_id, p.product_name FROM users u "
            "INNER JOIN orders o ON u.user_id = o.user_id "
            "LEFT JOIN products p ON o.product_id = p.product_id"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df_products = spark.table("products")\n'
        'df = df_users\n'
        'df = df.join(df_orders, df_users["user_id"] == df_orders["user_id"], "inner")\n'
        'df = df.join(df_products, df_orders["product_id"] == df_products["product_id"], "left")\n'
        'df = df.select("name", "order_id", "product_name")'
    )
        self.assertEqual(self.translator.translate(sql), expected)
        
    def test_multiple_joins_with_where(self):
        sql = (
            "SELECT u.name, o.order_id, p.product_name FROM users u "
            "INNER JOIN orders o ON u.user_id = o.user_id "
            "LEFT JOIN products p ON o.product_id = p.product_id "
            "WHERE o.order_date > '2023-01-01' AND p.category = 'Electronics'"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df_products = spark.table("products")\n'
        'df = df_users\n'
        'df = df.join(df_orders, df_users["user_id"] == df_orders["user_id"], "inner")\n'
        'df = df.join(df_products, df_orders["product_id"] == df_products["product_id"], "left")\n'
        'df = df.select("name", "order_id", "product_name")\n'
        'df = df.filter("order_date > \'2023-01-01\' AND category = \'Electronics\'")'
    )
        self.assertEqual(self.translator.translate(sql), expected)
        
    def test_three_way_join(self):
        sql = (
            "SELECT u.name, o.order_id, p.product_name, c.category_name FROM users u "
            "INNER JOIN orders o ON u.user_id = o.user_id "
            "LEFT JOIN products p ON o.product_id = p.product_id "
            "RIGHT JOIN categories c ON p.category_id = c.category_id"
        )
        expected = (
        'df_users = spark.table("users")\n'
        'df_orders = spark.table("orders")\n'
        'df_products = spark.table("products")\n'
        'df_categories = spark.table("categories")\n'
        'df = df_users\n'
        'df = df.join(df_orders, df_users["user_id"] == df_orders["user_id"], "inner")\n'
        'df = df.join(df_products, df_orders["product_id"] == df_products["product_id"], "left")\n'
        'df = df.join(df_categories, df_products["category_id"] == df_categories["category_id"], "right")\n'
        'df = df.select("name", "order_id", "product_name", "category_name")'
    )
        self.assertEqual(self.translator.translate(sql), expected)


if __name__ == "__main__":
    unittest.main()