CREATE KEYSPACE IF NOT EXISTS sales WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Витрина продаж по продуктам
CREATE TABLE IF NOT EXISTS sales.sales_by_product (
                                                      product_id int,
                                                      product_name text,
                                                      category_name text,
                                                      total_revenue double,
                                                      total_quantity bigint,
                                                      avg_rating double,
                                                      total_reviews bigint,
                                                      PRIMARY KEY (product_id, category_name)
);

-- Витрина продаж по клиентам
CREATE TABLE IF NOT EXISTS sales.sales_by_customer (
                                                       customer_id int,
                                                       first_name text,
                                                       last_name text,
                                                       customer_country text,
                                                       total_amount double,
                                                       avg_check double,
                                                       PRIMARY KEY (customer_id, customer_country)
);

-- Витрина продаж по времени
CREATE TABLE IF NOT EXISTS sales.sales_by_time (
                                                   sale_year int,
                                                   sale_month int,
                                                   total_revenue double,
                                                   total_quantity bigint,
                                                   avg_order_size double,
                                                   PRIMARY KEY (sale_year, sale_month)
);

-- Витрина продаж по магазинам
CREATE TABLE IF NOT EXISTS sales.sales_by_store (
                                                    store_id int,
                                                    store_name text,
                                                    store_city text,
                                                    store_country text,
                                                    total_revenue double,
                                                    total_quantity bigint,
                                                    avg_check double,
                                                    PRIMARY KEY (store_id, store_city)
);

-- Витрина продаж по поставщикам
CREATE TABLE IF NOT EXISTS sales.sales_by_supplier (
                                                       supplier_id int,
                                                       supplier_contact text,
                                                       supplier_country text,
                                                       total_revenue double,
                                                       avg_product_price double,
                                                       total_quantity bigint,
                                                       PRIMARY KEY (supplier_id, supplier_country)
);

-- Витрина качества продукции
CREATE TABLE IF NOT EXISTS sales.product_quality (
                                                     product_id int,
                                                     product_name text,
                                                     product_rating double,
                                                     total_reviews bigint,
                                                     total_revenue double,
                                                     total_quantity bigint,
                                                     PRIMARY KEY (product_id, product_rating)
);