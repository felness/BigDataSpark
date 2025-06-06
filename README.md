# 🚀 Проект по обработке больших данных

## 📋 Описание проекта
Проект представляет собой систему обработки и анализа данных с использованием Apache Spark и различных баз данных. Система позволяет загружать данные из PostgreSQL, обрабатывать их и сохранять результаты в различные хранилища данных.

## 🛠 Технологический стек
- **Apache Spark 3.5.3** - основной движок обработки данных
- **Python** - язык программирования для написания ETL-процессов
- **Docker** - контейнеризация приложения
- **Базы данных:**
  - PostgreSQL - источник данных
  - MongoDB - хранилище для отчетов
  - Cassandra - хранилище для отчетов
  - ClickHouse - хранилище для отчетов
  - Neo4j - графовая база данных
  - Valkey (Redis) - кэш-хранилище

## 📁 Структура проекта
```
.
├── app/                    # Основной код приложения
│   ├── main.py            # Точка входа
│   ├── transform/         # ETL-процессы
│   └── reports/          # Генерация отчетов
├── scripts/               # Скрипты для запуска
├── devops/               # DevOps конфигурации
├── mock_data/            # Тестовые данные
└── docker-compose.yaml   # Конфигурация Docker
```

## 🚀 Запуск проекта

### Предварительные требования
- Docker
- Docker Compose
- Python 3.x

### Шаги запуска
1. Запустите все контейнеры:
```bash
docker-compose up --build -d
```
Дождитесь запуска контейнера spark-submit (он зависит от всех остальных контейнеров)

2. Подключитесь к PostgreSQL через DBeaver:
- URL: `jdbc:postgresql://postgres:6433/sales_db`
- Логин: `admin`
- Пароль: `secret`
- База данных: `sales_db`

3. Импортируйте данные:
- Зайдите в базу sales_db
- Импортируйте данные из папки mock_data через механизм импорта
- Появится таблица mock_data с заполненными данными

4. Инициализируйте Neo4j:
```bash
docker exec -it neo4j-db bash /docker-entrypoint-initdb.d/init-db.sh
```

5. Запустите Spark контейнер:
```bash
docker exec -it spark-submit /bin/bash
```

6. Выполните задачи в следующем порядке:
```bash
# 1. Трансформация в снежинку
bash submit.sh transform

# 2. Генерация витрин в ClickHouse
bash submit.sh reports-clickhouse

# 3. Генерация витрин в Cassandra
bash submit.sh reports-cassandra

# 4. Генерация витрин в Neo4j
bash submit.sh reports-neo4j

# 5. Генерация витрин в MongoDB
bash submit.sh reports-mongo

# 6. Генерация витрин в Valkey
bash submit.sh reports-valkey
```

## 📊 Генерируемые отчеты
- выполнена лаба по реализации etl пайплана через спарк - трансвормация данных из postgres в postgres в виде снежинки 
- далее последовательный запуск генерации витрин данных 
- 1) в clickhouse
- 2) mondodb
- 3) cassandra
- 4) neo4j
- 5) valkey

## 🔧 Конфигурация
Основные настройки находятся в файле `config.yaml`:
- Параметры подключения к базам данных
- Настройки Spark
- Параметры ETL-процессов

## 📈 Мониторинг
- Логи приложения доступны в файле `/app/etl.log`
- Spark UI доступен по адресу: `http://localhost:8080`

## 🔐 Безопасность
- Все пароли и чувствительные данные хранятся в конфигурационном файле
- Используются безопасные соединения с базами данных
- Контейнеры изолированы друг от друга

## 🤝 Вклад в проект
1. Форкните репозиторий
2. Создайте ветку для ваших изменений
3. Внесите изменения
4. Создайте pull request
