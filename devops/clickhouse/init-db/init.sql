-- Создание базы данных, если она не существует
CREATE DATABASE IF NOT EXISTS sales_db;

-- Витрина продаж по продуктам
CREATE TABLE sales_db.sales_by_product (
                                           product_id Int32,
                                           product_name String,
                                           category_name String,
                                           total_revenue Decimal(15,2),
                                           total_quantity Int32,
                                           avg_rating Float32,
                                           total_reviews Int32
)
    ENGINE = MergeTree()
ORDER BY (product_id);

-- Витрина продаж по клиентам
CREATE TABLE sales_db.sales_by_customer (
                                            customer_id Int32,
                                            first_name String,
                                            last_name String,
                                            customer_country String,
                                            total_amount Decimal(15,2),
                                            avg_check Decimal(15,2)
)
    ENGINE = MergeTree()
ORDER BY (customer_id);

-- Витрина продаж по времени
CREATE TABLE sales_db.sales_by_time (
                                        sale_year Int32,
                                        sale_month Int32,
                                        total_revenue Decimal(15,2),
                                        total_quantity Int32,
                                        avg_order_size Decimal(15,2)
)
    ENGINE = MergeTree()
ORDER BY (sale_year, sale_month);

-- Витрина продаж по магазинам
CREATE TABLE sales_db.sales_by_store (
                                         store_id Int32,
                                         store_name String,
                                         store_city String,
                                         store_country String,
                                         total_revenue Decimal(15,2),
                                         total_quantity Int32,
                                         avg_check Decimal(15,2)
)
    ENGINE = MergeTree()
ORDER BY (store_id);

-- Витрина продаж по поставщикам
CREATE TABLE sales_db.sales_by_supplier (
                                            supplier_id Int32,
                                            supplier_contact String,
                                            supplier_country String,
                                            total_revenue Decimal(15,2),
                                            avg_product_price Decimal(15,2),
                                            total_quantity Int32
)
    ENGINE = MergeTree()
ORDER BY (supplier_id);

-- Витрина качества продукции
CREATE TABLE sales_db.product_quality (
                                          product_id Int32,
                                          product_name String,
                                          product_rating Float32,
                                          total_reviews Int32,
                                          total_revenue Decimal(15,2),
                                          total_quantity Int32
)
    ENGINE = MergeTree()
ORDER BY (product_id);