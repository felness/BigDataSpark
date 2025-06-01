import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

def reports_neo4j(spark, config):
    """
    Выполняет обработку данных из PostgreSQL и записывает витрины в Neo4j.

    Args:
        spark: SparkSession объект
        config: Словарь с конфигурацией из config.yaml
    """
    # Проверка типа spark
    if not isinstance(spark, SparkSession):
        logger.error("Provided spark is not a SparkSession object")
        raise TypeError("spark must be a SparkSession object")

    # Настройки Neo4j
    try:
        neo4j_config = config["datasource"]["neo4j"]
        neo4j_url = neo4j_config["uri"]
        neo4j_user = neo4j_config["user"]
        neo4j_password = neo4j_config["password"]
        neo4j_database = "salesdb"  # Фиксированная база данных, как в Java
        logger.info("Neo4j connection settings applied")
    except KeyError as e:
        logger.error(f"Missing Neo4j configuration key: {e}")
        raise

    # Установка legacy time parser policy, как в Java
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Чтение данных из PostgreSQL
    try:
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
    except Exception as e:
        logger.error(f"Failed to read from PostgreSQL: {str(e)}")
        raise

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

    # 1. Витрина: Продажи по продуктам
    logger.info("Создание узлов продуктов и категорий...")
    products_with_sales_query = """
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
    products_with_sales = spark.sql(products_with_sales_query)
    write_neo4j(products_with_sales, neo4j_url, neo4j_user, neo4j_password, neo4j_database,
                labels="Product",
                node_keys="product_id",
                node_properties="product_name,category_name,total_revenue,total_quantity,avg_rating,total_reviews")

    write_neo4j(product_categories, neo4j_url, neo4j_user, neo4j_password, neo4j_database,
                labels="Category",
                node_keys="category_id")

    # 2. Витрина: Продажи по клиентам и отношения покупок
    logger.info("Создание узлов клиентов и отношений покупок...")
    customers_query = """
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
    customers = spark.sql(customers_query)
    write_neo4j(customers, neo4j_url, neo4j_user, neo4j_password, neo4j_database,
                labels="Customer",
                node_keys="customer_id",
                node_properties="first_name,last_name,customer_country,total_amount,avg_check")

    customer_purchases_query = """
    SELECT c.customer_id, fs.product_id,
           CAST(SUM(fs.total_amount) AS DOUBLE) AS total_spent
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_id = c.customer_id
    GROUP BY c.customer_id, fs.product_id
    """
    customer_purchases = spark.sql(customer_purchases_query)
    write_neo4j(customer_purchases, neo4j_url, neo4j_user, neo4j_password, neo4j_database,
                relationship="PURCHASED",
                relationship_save_strategy="KEYS",
                relationship_source_labels="Customer",
                relationship_source_node_keys="customer_id",
                relationship_target_labels="Product",
                relationship_target_node_keys="product_id",
                relationship_properties="total_spent")

    # 3. Витрина: Продажи по времени
    logger.info("Создание временных узлов...")
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
    write_neo4j(sales_by_time, neo4j_url, neo4j_user, neo4j_password, neo4j_database,
                labels="TimePeriod",
                node_keys="sale_year,sale_month",
                node_properties="total_revenue,total_quantity,avg_order_size")

    # 4. Витрина: Продажи по магазинам
    logger.info("Создание узлов магазинов...")
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
    write_neo4j(sales_by_store, neo4j_url, neo4j_user, neo4j_password, neo4j_database,
                labels="Store",
                node_keys="store_id",
                node_properties="store_name,store_city,store_country,total_revenue,total_quantity,avg_check")

    # 5. Витрина: Продажи по поставщикам
    logger.info("Создание узлов поставщиков...")
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
    write_neo4j(sales_by_supplier, neo4j_url, neo4j_user, neo4j_password, neo4j_database,
                labels="Supplier",
                node_keys="supplier_id",
                node_properties="supplier_contact,supplier_country,total_revenue,avg_product_price,total_quantity")

    # 6. Витрина: Качество продукции
    logger.info("Создание свойств качества продуктов...")
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
    write_neo4j(product_quality, neo4j_url, neo4j_user, neo4j_password, neo4j_database,
                labels="Product",
                node_keys="product_id",
                node_properties="product_name,product_rating,total_reviews,total_revenue,total_quantity")

    logger.info("Все витрины успешно созданы в Neo4j")

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
    try:
        return spark.read \
            .format("jdbc") \
            .option("url", config["datasource"]["postgres"]["url"]) \
            .option("dbtable", table) \
            .option("user", config["datasource"]["postgres"]["user"]) \
            .option("password", config["datasource"]["postgres"]["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()
    except KeyError as e:
        logger.error(f"Missing PostgreSQL configuration key: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to read table {table} from PostgreSQL: {str(e)}")
        raise

def write_neo4j(df, url, user, password, database, **options):
    """
    Записывает DataFrame в Neo4j как узлы или отношения.

    Args:
        df: PySpark DataFrame
        url: URL Neo4j (bolt-протокол)
        user: Имя пользователя Neo4j
        password: Пароль Neo4j
        database: Название базы данных Neo4j
        **options: Дополнительные параметры (labels, node_keys, relationship, и т.д.)
    """
    logger.info(f"Writing to Neo4j with options: {options}")
    try:
        writer = df.write \
            .format("org.neo4j.spark.DataSource") \
            .option("url", url) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", user) \
            .option("authentication.basic.password", password) \
            .option("database", database) \
            .option("batch.size", "1000") \
            .mode("overwrite")

        # Добавление опций
        for key, value in options.items():
            if key == "labels":
                writer = writer.option("labels", value)
            elif key == "node_keys":
                writer = writer.option("node.keys", value)
            elif key == "node_properties":
                writer = writer.option("node.properties", value)
            elif key == "relationship":
                writer = writer.option("relationship", value)
            elif key == "relationship_save_strategy":
                writer = writer.option("relationship.save.strategy", value)
            elif key == "relationship_source_labels":
                writer = writer.option("relationship.source.labels", value)
            elif key == "relationship_source_node_keys":
                writer = writer.option("relationship.source.node.keys", value)
            elif key == "relationship_target_labels":
                writer = writer.option("relationship.target.labels", value)
            elif key == "relationship_target_node_keys":
                writer = writer.option("relationship.target.node.keys", value)
            elif key == "relationship_properties":
                writer = writer.option("relationship.properties", value)
            else:
                logger.warning(f"Unknown option {key} with value {value}")

        writer.save()
    except Exception as e:
        logger.error(f"Failed to write to Neo4j: {str(e)}")
        raise