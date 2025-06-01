import logging
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def reports_valkey(spark, config):
    """
    Process data from PostgreSQL and write data marts to Valkey.

    Args:
        spark: SparkSession object
        config: Configuration dictionary from config.yaml
    """
    # Valkey settings
    valkey_host = config["datasource"]["valkey"]["host"]
    valkey_port = config["datasource"]["valkey"]["port"]
    valkey_db = "1"  # Fixed index as in Java

    # Load data from PostgreSQL
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

    # Create temporary views
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

    # Data Mart: Sales by Product
    logger.info("Creating sales by product data mart...")
    sales_by_product_query = """
    SELECT p.product_id, p.product_name, pc.category_name,
           CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue,
           SUM(fs.product_quantity) AS total_quantity,
           CAST(AVG(ps.product_rating) AS DOUBLE) AS avg_rating,
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
    write_to_valkey(sales_by_product, valkey_host, valkey_port, valkey_db, "sales_by_product", ["product_id"])

    # Data Mart: Sales by Customer
    logger.info("Creating sales by customer data mart...")
    sales_by_customer_query = """
    SELECT c.customer_id, c.first_name, c.last_name, cci.customer_country,
           CAST(SUM(fs.total_amount) AS DOUBLE) AS total_amount,
           CAST(AVG(fs.total_amount) AS DOUBLE) AS avg_check
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_id = c.customer_id
    JOIN customer_contact_info cci ON c.customer_id = cci.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, cci.customer_country
    ORDER BY total_amount DESC
    LIMIT 10
    """
    sales_by_customer = spark.sql(sales_by_customer_query)
    write_to_valkey(sales_by_customer, valkey_host, valkey_port, valkey_db, "sales_by_customer", ["customer_id"])

    # Data Mart: Sales by Time
    logger.info("Creating sales by time data mart...")
    sales_by_time_query = """
    SELECT YEAR(fs.sale_date) AS sale_year, MONTH(fs.sale_date) AS sale_month,
           CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue,
           SUM(fs.product_quantity) AS total_quantity,
           CAST(AVG(fs.total_amount) AS DOUBLE) AS avg_order_size
    FROM fact_sales fs
    GROUP BY YEAR(fs.sale_date), MONTH(fs.sale_date)
    ORDER BY sale_year, sale_month
    """
    sales_by_time = spark.sql(sales_by_time_query)
    write_to_valkey(sales_by_time, valkey_host, valkey_port, valkey_db, "sales_by_time", ["sale_year", "sale_month"])

    # Data Mart: Sales by Store
    logger.info("Creating sales by store data mart...")
    sales_by_store_query = """
    SELECT s.store_id, s.store_name, s.store_city, si.store_country,
           CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue,
           SUM(fs.product_quantity) AS total_quantity,
           CAST(AVG(fs.total_amount) AS DOUBLE) AS avg_check
    FROM fact_sales fs
    JOIN dim_store s ON fs.store_id = s.store_id
    JOIN store_info si ON s.store_id = si.store_id
    GROUP BY s.store_id, s.store_name, s.store_city, si.store_country
    ORDER BY total_revenue DESC
    LIMIT 5
    """
    sales_by_store = spark.sql(sales_by_store_query)
    write_to_valkey(sales_by_store, valkey_host, valkey_port, valkey_db, "sales_by_store", ["store_id"])

    # Data Mart: Sales by Supplier
    logger.info("Creating sales by supplier data mart...")
    sales_by_supplier_query = """
    SELECT sup.supplier_id, sup.supplier_contact, si.supplier_country,
           CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue,
           CAST(AVG(p.product_price) AS DOUBLE) AS avg_product_price,
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
    write_to_valkey(sales_by_supplier, valkey_host, valkey_port, valkey_db, "sales_by_supplier", ["supplier_id"])

    # Data Mart: Product Quality
    logger.info("Creating product quality data mart...")
    product_quality_query = """
    SELECT p.product_id, p.product_name,
           CAST(ps.product_rating AS DOUBLE) AS product_rating,
           ps.product_reviews AS total_reviews,
           CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue,
           SUM(fs.product_quantity) AS total_quantity
    FROM fact_sales fs
    JOIN dim_products p ON fs.product_id = p.product_id
    JOIN product_statistics ps ON p.product_id = ps.product_id
    GROUP BY p.product_id, p.product_name, ps.product_rating, ps.product_reviews
    ORDER BY ps.product_rating DESC
    """
    product_quality = spark.sql(product_quality_query)
    write_to_valkey(product_quality, valkey_host, valkey_port, valkey_db, "product_quality", ["product_id"])

    logger.info("All data marts successfully written to Valkey")

def read_jdbc(spark, config, table):
    """
    Read a table from PostgreSQL via JDBC.

    Args:
        spark: SparkSession object
        config: Configuration dictionary
        table: Table name

    Returns:
        DataFrame with table data
    """
    return spark.read \
        .format("jdbc") \
        .option("url", config["datasource"]["postgres"]["url"]) \
        .option("dbtable", table) \
        .option("user", config["datasource"]["postgres"]["user"]) \
        .option("password", config["datasource"]["postgres"]["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def write_to_valkey(df, host, port, db, table_prefix, key_columns):
    """
    Write DataFrame to Valkey.

    Args:
        df: PySpark DataFrame
        host: Valkey host
        port: Valkey port
        db: Valkey database index
        table_prefix: Prefix for keys in Valkey
        key_columns: List of columns to form the key
    """
    if len(key_columns) > 1:
        key_column_name = "key_column"
        df = df.withColumn(key_column_name, F.concat_ws(":", *[F.col(c) for c in key_columns]))
    else:
        key_column_name = key_columns[0]

    df.write \
        .format("org.apache.spark.sql.redis") \
        .option("host", host) \
        .option("port", port) \
        .option("db", db) \
        .option("table", table_prefix) \
        .option("key.column", key_column_name) \
        .option("spark.redis.batch.size", "1000") \
        .mode("overwrite") \
        .save()