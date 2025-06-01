import sys
import logging
import yaml
from pyspark.sql import SparkSession
from transform.etl_to_snowflake_job import run_etl_to_snowflake
from reports.clickhouse import reports_clickhouse
from reports.cassandra import reports_cassandra
from reports.mongo import reports_mongodb
from reports.neo4j import reports_neo4j
from reports.valkey import reports_valkey

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/app/etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def init_spark(config, mode):
    """Инициализация SparkSession с учетом режима."""
    builder = SparkSession.builder \
        .appName(config["spark"]["app-name"]) \
        .master(config["spark"]["master"]) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.ui.enabled", str(config["spark"]["ui"]["enabled"]).lower())

    # Специфичная конфигурация для MongoDB
    if mode == "mongodb":
        logger.info("Applying MongoDB-specific Spark configuration")
        mongo_uri = "mongodb://user:pass@mongo-db:27017/sales_db?authSource=sales_db"
        builder = builder \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.mongodb.read.connection.uri", mongo_uri) \
            .config("spark.mongodb.write.connection.uri", mongo_uri) \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.logConf", "true")

    return builder.getOrCreate()

def main():
    with open("/app/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Получение режима
    mode = sys.argv[1] if len(sys.argv) > 1 else "snowflake"

    # Создание SparkSession с учетом режима
    spark = init_spark(config, mode)

    try:
        if mode == "snowflake":
            logger.info("Запуск трансформации в снежинную схему...")
            run_etl_to_snowflake(spark, config)
        elif mode == "clickhouse":
            logger.info("Запуск генерации витрин в кликхаусе...")
            reports_clickhouse(spark, config)
        elif mode == "cassandra":
            logger.info("Запуск генерации витрин в cassandra...")
            reports_cassandra(spark, config)
        elif mode == "mongodb":
            logger.info("Запуск генерации витрин в mongo...")
            reports_mongodb(spark, config)
        elif mode == "neo4j":
            logger.info("Запуск генерации отчетов в neo4j...")
            reports_neo4j(spark, config)
        elif mode == "valkey":
            logger.info("Запуск генерации ответов в valkey...")
            reports_valkey(spark, config)
        else:
            logger.error(f"Неизвестный режим: {mode}")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Ошибка в режиме {mode}: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()