import logging
import yaml
from pyspark.sql import SparkSession

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_jdbc(spark, config, table):
    """Чтение данных из PostgreSQL через JDBC."""
    return spark.read \
        .format("jdbc") \
        .option("url", config["datasource"]["postgres"]["url"]) \
        .option("dbtable", table) \
        .option("user", config["datasource"]["postgres"]["user"]) \
        .option("password", config["datasource"]["postgres"]["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def write_jdbc(df, config, table, mode="append"):
    """Запись данных в PostgreSQL через JDBC."""
    df.write \
        .format("jdbc") \
        .option("url", config["datasource"]["postgres"]["url"]) \
        .option("dbtable", table) \
        .option("user", config["datasource"]["postgres"]["user"]) \
        .option("password", config["datasource"]["postgres"]["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()

def run_etl_to_snowflake(spark, config):
    """Основная функция ETL для создания снежинной схемы."""
    try:
        # Чтение исходных данных из таблицы mock_data
        logger.info("Чтение mock_data...")
        mock_data = read_jdbc(spark, config, "mock_data")
        mock_data.cache()
        mock_data.createOrReplaceTempView("mock_data")

        # 1. dim_customer
        logger.info("Заполнение dim_customer...")
        dim_customer_query = """
        SELECT DISTINCT customer_first_name AS first_name,
                        customer_last_name AS last_name,
                        CAST(customer_age AS INTEGER) AS age
        FROM mock_data
        """
        dim_customer = spark.sql(dim_customer_query)
        write_jdbc(dim_customer, config, "dim_customer")

        # Чтение dim_customer с сгенерированными ID
        dim_customer_with_id = read_jdbc(spark, config, "dim_customer")
        dim_customer_with_id.createOrReplaceTempView("dim_customer")

        # 2. customer_contact_info
        logger.info("Заполнение customer_contact_info...")
        customer_contact_info_query = """
        SELECT c.customer_id, m.customer_email, m.customer_country, m.customer_postal_code
        FROM mock_data m
        JOIN dim_customer c ON m.customer_first_name = c.first_name
                            AND m.customer_last_name = c.last_name
                            AND CAST(m.customer_age AS INTEGER) = c.age
        """
        customer_contact_info = spark.sql(customer_contact_info_query)
        write_jdbc(customer_contact_info, config, "customer_contact_info")

        # 3. customer_pet_info
        logger.info("Заполнение customer_pet_info...")
        customer_pet_info_query = """
        SELECT c.customer_id, m.customer_pet_type AS pet_type, m.customer_pet_name AS pet_name,
               m.customer_pet_breed AS pet_breed
        FROM mock_data m
        JOIN dim_customer c ON m.customer_first_name = c.first_name
                            AND m.customer_last_name = c.last_name
                            AND CAST(m.customer_age AS INTEGER) = c.age
        """
        customer_pet_info = spark.sql(customer_pet_info_query)
        write_jdbc(customer_pet_info, config, "customer_pet_info")

        # 4. dim_seller
        logger.info("Заполнение dim_seller...")
        dim_seller_query = """
        SELECT DISTINCT seller_first_name, seller_last_name
        FROM mock_data
        """
        dim_seller = spark.sql(dim_seller_query)
        write_jdbc(dim_seller, config, "dim_seller")

        # Чтение dim_seller с ID
        dim_seller_with_id = read_jdbc(spark, config, "dim_seller")
        dim_seller_with_id.createOrReplaceTempView("dim_seller")

        # 5. seller_contact_info
        logger.info("Заполнение seller_contact_info...")
        seller_contact_info_query = """
        SELECT s.seller_id, m.seller_email, m.seller_country, m.seller_postal_code
        FROM mock_data m
        JOIN dim_seller s ON m.seller_first_name = s.seller_first_name
                          AND m.seller_last_name = s.seller_last_name
        """
        seller_contact_info = spark.sql(seller_contact_info_query)
        write_jdbc(seller_contact_info, config, "seller_contact_info")

        # 6. product_categories
        logger.info("Заполнение product_categories...")
        product_categories_query = """
        SELECT DISTINCT product_category AS category_name
        FROM mock_data
        """
        product_categories = spark.sql(product_categories_query)
        write_jdbc(product_categories, config, "product_categories")

        # Чтение product_categories с ID
        product_categories_with_id = read_jdbc(spark, config, "product_categories")
        product_categories_with_id.createOrReplaceTempView("product_categories")

        # 7. dim_products
        logger.info("Заполнение dim_products...")
        dim_products_query = """
        SELECT DISTINCT m.product_name,
                        CAST(m.product_price AS FLOAT) AS product_price,
                        pc.category_id AS product_category,
                        m.customer_pet_type AS pet_category,
                        CAST(m.product_weight AS FLOAT) AS product_weight,
                        m.product_color, m.product_size, m.product_material,
                        m.product_brand, m.product_description
        FROM mock_data m
        JOIN product_categories pc ON m.product_category = pc.category_name
        """
        dim_products = spark.sql(dim_products_query)
        write_jdbc(dim_products, config, "dim_products")

        # Чтение dim_products с ID
        dim_products_with_id = read_jdbc(spark, config, "dim_products")
        dim_products_with_id.createOrReplaceTempView("dim_products")

        # 8. product_statistics
        logger.info("Заполнение product_statistics...")
        product_statistics_query = """
        SELECT p.product_id,
               CAST(m.product_rating AS FLOAT) AS product_rating,
               CAST(m.product_reviews AS INTEGER) AS product_reviews,
               TO_DATE(m.product_release_date, 'MM/dd/yyyy') AS product_release_date,
               TO_DATE(m.product_expiry_date, 'MM/dd/yyyy') AS product_expiry_date
        FROM mock_data m
        JOIN dim_products p ON m.product_name = p.product_name
                            AND CAST(m.product_price AS FLOAT) = p.product_price
        """
        product_statistics = spark.sql(product_statistics_query).dropDuplicates(["product_id"])
        write_jdbc(product_statistics, config, "product_statistics")

        # 9. dim_store
        logger.info("Заполнение dim_store...")
        dim_store_query = """
        SELECT DISTINCT store_name, store_location, store_city
        FROM mock_data
        """
        dim_store = spark.sql(dim_store_query)
        write_jdbc(dim_store, config, "dim_store")

        # Чтение dim_store с ID
        dim_store_with_id = read_jdbc(spark, config, "dim_store")
        dim_store_with_id.createOrReplaceTempView("dim_store")

        # 10. store_info
        logger.info("Заполнение store_info...")
        store_info_query = """
        SELECT s.store_id, m.store_state, m.store_country, m.store_phone, m.store_email
        FROM mock_data m
        JOIN dim_store s ON m.store_name = s.store_name
                         AND m.store_location = s.store_location
                         AND m.store_city = s.store_city
        """
        store_info = spark.sql(store_info_query)
        write_jdbc(store_info, config, "store_info")

        # 11. dim_supplier
        logger.info("Заполнение dim_supplier...")
        dim_supplier_query = """
        SELECT DISTINCT supplier_contact, supplier_city, supplier_address
        FROM mock_data
        """
        dim_supplier = spark.sql(dim_supplier_query)
        write_jdbc(dim_supplier, config, "dim_supplier")

        # Чтение dim_supplier с ID
        dim_supplier_with_id = read_jdbc(spark, config, "dim_supplier")
        dim_supplier_with_id.createOrReplaceTempView("dim_supplier")

        # 12. supplier_info
        logger.info("Заполнение supplier_info...")
        supplier_info_query = """
        SELECT s.supplier_id, m.supplier_email, m.supplier_phone, m.supplier_country
        FROM mock_data m
        JOIN dim_supplier s ON m.supplier_contact = s.supplier_contact
                            AND m.supplier_city = s.supplier_city
                            AND m.supplier_address = s.supplier_address
        """
        supplier_info = spark.sql(supplier_info_query)
        write_jdbc(supplier_info, config, "supplier_info")

        # 13. fact_sales
        logger.info("Заполнение fact_sales...")
        fact_sales_query = """
        SELECT c.customer_id,
               p.product_id,
               sl.seller_id,
               s.store_id,
               sup.supplier_id,
               TO_DATE(m.sale_date, 'MM/dd/yyyy') AS sale_date,
               CAST(m.product_quantity AS INTEGER) AS product_quantity,
               CAST(m.sale_total_price AS DECIMAL(10,2)) AS total_amount
        FROM mock_data m
        JOIN dim_customer c ON m.customer_first_name = c.first_name
                            AND m.customer_last_name = c.last_name
                            AND CAST(m.customer_age AS INTEGER) = c.age
        JOIN dim_products p ON m.product_name = p.product_name
                            AND CAST(m.product_price AS FLOAT) = p.product_price
        JOIN dim_store s ON m.store_name = s.store_name
                         AND m.store_location = s.store_location
                         AND m.store_city = s.store_city
        JOIN dim_supplier sup ON m.supplier_contact = sup.supplier_contact
                              AND m.supplier_city = sup.supplier_city
                              AND m.supplier_address = sup.supplier_address
        JOIN dim_seller sl ON m.seller_first_name = sl.seller_first_name
                           AND m.seller_last_name = sl.seller_last_name
        """
        fact_sales = spark.sql(fact_sales_query)
        write_jdbc(fact_sales, config, "fact_sales")

        # Освобождение кэша
        logger.info("Освобождение кэша mock_data...")
        mock_data.unpersist()

    except Exception as e:
        logger.error(f"Ошибка в ETL-процессе: {e}")
        raise

if __name__ == "__main__":
    # Загрузка конфигурации из config.yaml
    with open("/app/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Создание SparkSession с параметрами из config.yaml
    spark = SparkSession.builder \
        .appName(config["spark"]["app-name"]) \
        .master(config["spark"]["master"]) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.ui.enabled", str(config["spark"]["ui"]["enabled"]).lower()) \
        .getOrCreate()

    # Установка уровня логирования Spark
    spark.sparkContext.setLogLevel("WARN")

    # Запуск ETL-процесса
    run_etl_to_snowflake(spark, config)

    # Остановка SparkSession
    spark.stop()