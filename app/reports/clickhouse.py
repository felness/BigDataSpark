import logging

logger = logging.getLogger(__name__)

def reports_clickhouse(spark, config):
    """
    Выполняет обработку данных из PostgreSQL и записывает витрины в ClickHouse.

    Args:
        spark: SparkSession объект
        config: Словарь с конфигурацией из config.yaml
    """
    # Чтение данных из PostgreSQL
    fact_sales = read_jdbc(spark, config, "fact_sales")
    dim_products = read_jdbc(spark, config, "dim_products")
    product_categories = read_jdbc(spark, config, "product_categories")
    dim_customer = read_jdbc(spark, config, "dim_customer")
    customer_contact_info = read_jdbc(spark, config, "customer_contact_info")
    dim_store = read_jdbc(spark, config, "dim_store")
    store_info = read_jdbc(spark, config, "store_info")
    dim_supplier = read_jdbc(spark, config, "dim_supplier")
    supplier_info = read_jdbc(spark, config, "supplier_info")
    product_statistics = read_jdbc(spark, config, "product_statistics")

    # Создание временных представлений
    fact_sales.createOrReplaceTempView("fact_sales")
    dim_products.createOrReplaceTempView("dim_products")
    product_categories.createOrReplaceTempView("product_categories")
    dim_customer.createOrReplaceTempView("dim_customer")
    customer_contact_info.createOrReplaceTempView("customer_contact_info")
    dim_store.createOrReplaceTempView("dim_store")
    store_info.createOrReplaceTempView("store_info")
    dim_supplier.createOrReplaceTempView("dim_supplier")
    supplier_info.createOrReplaceTempView("supplier_info")
    product_statistics.createOrReplaceTempView("product_statistics")

    # Витрина продаж по продуктам
    sales_by_product_query = """
    SELECT p.product_id, p.product_name, pc.category_name,
           CAST(SUM(fs.total_amount) AS Decimal(15,2)) AS total_revenue,
           SUM(fs.product_quantity) AS total_quantity,
           AVG(ps.product_rating) AS avg_rating,
           SUM(ps.product_reviews) AS total_reviews
    FROM fact_sales fs
    JOIN dim_products p ON fs.product_id = p.product_id
    JOIN product_categories pc ON p.product_category = pc.category_id
    JOIN product_statistics ps ON p.product_id = ps.product_id
    GROUP BY p.product_id, p.product_name, pc.category_name
    ORDER BY total_quantity DESC
    LIMIT 10
    """
    sales_by_product = spark.sql(sales_by_product_query)
    write_jdbc(sales_by_product, config, "sales_by_product")


    # Витрина продаж по клиентам
    sales_by_customer_query = """
    SELECT c.customer_id, c.first_name, c.last_name, cci.customer_country,
           CAST(SUM(fs.total_amount) AS Decimal(15,2)) AS total_amount,
           CAST(AVG(fs.total_amount) AS Decimal(15,2)) AS avg_check
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_id = c.customer_id
    JOIN customer_contact_info cci ON c.customer_id = cci.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, cci.customer_country
    ORDER BY total_amount DESC
    LIMIT 10
    """
    sales_by_customer = spark.sql(sales_by_customer_query)
    write_jdbc(sales_by_customer, config, "sales_by_customer")

    # Витрина продаж по времени
    sales_by_time_query = """
    SELECT YEAR(fs.sale_date) AS sale_year, MONTH(fs.sale_date) AS sale_month,
           CAST(SUM(fs.total_amount) AS Decimal(15,2)) AS total_revenue,
           SUM(fs.product_quantity) AS total_quantity,
           CAST(AVG(fs.total_amount) AS Decimal(15,2)) AS avg_order_size
    FROM fact_sales fs
    GROUP BY YEAR(fs.sale_date), MONTH(fs.sale_date)
    ORDER BY sale_year, sale_month
    """
    sales_by_time = spark.sql(sales_by_time_query)
    write_jdbc(sales_by_time, config, "sales_by_time")

    # Витрина продаж по магазинам
    sales_by_store_query = """
    SELECT s.store_id, s.store_name, s.store_city, si.store_country,
           CAST(SUM(fs.total_amount) AS Decimal(15,2)) AS total_revenue,
           SUM(fs.product_quantity) AS total_quantity,
           CAST(AVG(fs.total_amount) AS Decimal(15,2)) AS avg_check
    FROM fact_sales fs
    JOIN dim_store s ON fs.store_id = s.store_id
    JOIN store_info si ON s.store_id = si.store_id
    GROUP BY s.store_id, s.store_name, s.store_city, si.store_country
    ORDER BY total_revenue DESC
    LIMIT 5
    """
    sales_by_store = spark.sql(sales_by_store_query)
    write_jdbc(sales_by_store, config, "sales_by_store")

    # Витрина продаж по поставщикам
    sales_by_supplier_query = """
    SELECT sup.supplier_id, sup.supplier_contact, si.supplier_country,
           CAST(SUM(fs.total_amount) AS Decimal(15,2)) AS total_revenue,
           CAST(AVG(p.product_price) AS Decimal(15,2)) AS avg_product_price,
           SUM(fs.product_quantity) AS total_quantity
    FROM fact_sales fs
    JOIN dim_supplier sup ON fs.supplier_id = sup.supplier_id
    JOIN supplier_info si ON sup.supplier_id = si.supplier_id
    JOIN dim_products p ON fs.product_id = p.product_id
    GROUP BY sup.supplier_id, sup.supplier_contact, si.supplier_country
    ORDER BY total_revenue DESC
    LIMIT 5
    """
    sales_by_supplier = spark.sql(sales_by_supplier_query)
    write_jdbc(sales_by_supplier, config, "sales_by_supplier")

    # Витрина качества продукции
    product_quality_query = """
    SELECT p.product_id, p.product_name, ps.product_rating,
           ps.product_reviews AS total_reviews,
           CAST(SUM(fs.total_amount) AS Decimal(15,2)) AS total_revenue,
           SUM(fs.product_quantity) AS total_quantity
    FROM fact_sales fs
    JOIN dim_products p ON fs.product_id = p.product_id
    JOIN product_statistics ps ON p.product_id = ps.product_id
    GROUP BY p.product_id, p.product_name, ps.product_rating, ps.product_reviews
    ORDER BY ps.product_rating DESC
    """
    product_quality = spark.sql(product_quality_query)
    write_jdbc(product_quality, config, "product_quality")

def read_jdbc(spark, config, table):
    """
    Читает таблицу из PostgreSQL через JDBC.

    Args:
        spark: SparkSession объект
        config: Словарь с конфигурацией
        table: Название таблицы

    Returns:
        DataFrame с данными из таблицы
    """
    return spark.read \
        .format("jdbc") \
        .option("url", config["datasource"]["postgres"]["url"]) \
        .option("dbtable", table) \
        .option("user", config["datasource"]["postgres"]["user"]) \
        .option("password", config["datasource"]["postgres"]["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def write_jdbc(df, config, table):
    """
    Записывает DataFrame в ClickHouse через JDBC.

    Args:
        df: PySpark DataFrame
        config: Словарь с конфигурацией
        table: Название таблицы в ClickHouse
    """
    df.write \
        .format("jdbc") \
        .option("url", config["datasource"]["clickhouse"]["url"]) \
        .option("dbtable", table) \
        .option("user", config["datasource"]["clickhouse"]["user"]) \
        .option("password", config["datasource"]["clickhouse"]["password"]) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()