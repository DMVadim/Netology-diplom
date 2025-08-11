from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from airflow.exceptions import AirflowException
import time
import logging
import pandas as pd
import hashlib
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
}

# Конфигурация API праздников
HOLIDAY_API_KEY = "8f2c957b-8f29-4128-b7ac-d6230dedb51b"
COUNTRY = "MM"  # Код страны (Мьянма)
YEAR = 2024     # Год для получения праздников

def get_clickhouse_client():
    """Создаёт подключение к ClickHouse"""
    return Client(
        host='clickhouse',
        port=9000,
        user='admin',
        password='admin',
        database='default',  # Подключаемся к default, чтобы создавать базы
        connect_timeout=10,
        settings={
            'connect_timeout': 10,
            'send_receive_timeout': 30,
            'max_execution_time': 30,
        }
    )

def init_clickhouse_db():
    """Инициализирует структуру БД в ClickHouse (слои NDS и DDS)"""
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            ch_client = get_clickhouse_client()
            ch_client.execute('SELECT 1')  # Проверка подключения
            
            # Создаём базы данных
            init_queries = [
                # Создаём схему NDS (сырой слой)
                "CREATE DATABASE IF NOT EXISTS retail_nds",
                
                # Создаём схему DDS (витрины данных)
                "CREATE DATABASE IF NOT EXISTS retail_dwh",
                
                ##############################################
                # Таблицы в NDS (нормализованные сырые данные)
                ##############################################
                
                # Таблица городов
                """CREATE TABLE IF NOT EXISTS retail_nds.cities (
                    city_id String,
                    city String,
                    PRIMARY KEY (city_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (city_id)""",
                
                # Таблица типов клиентов
                """CREATE TABLE IF NOT EXISTS retail_nds.customer_types (
                    customer_type_id String,
                    type_name String,
                    PRIMARY KEY (customer_type_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (customer_type_id)""",
                
                # Таблица гендеров
                """CREATE TABLE IF NOT EXISTS retail_nds.genders (
                    gender_id String,
                    gender_name String,
                    PRIMARY KEY (gender_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (gender_id)""",
                
                # Таблица линеек продуктов
                """CREATE TABLE IF NOT EXISTS retail_nds.product_lines (
                    product_line_id String,
                    line_name String,
                    PRIMARY KEY (product_line_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (product_line_id)""",
                
                # Таблица методов оплаты
                """CREATE TABLE IF NOT EXISTS retail_nds.payment_methods (
                    payment_method_id String,
                    method_name String,
                    PRIMARY KEY (payment_method_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (payment_method_id)""",
                
                # Таблица клиентов
                """CREATE TABLE IF NOT EXISTS retail_nds.customers (
                    customer_id UInt32,
                    customer_type_id String,
                    gender_id String,
                    PRIMARY KEY (customer_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (customer_id)""",
                
                # Таблица филиалов
                """CREATE TABLE IF NOT EXISTS retail_nds.branches (
                    branch_id String,
                    branch_name String,
                    city_id String,
                    PRIMARY KEY (branch_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (branch_id)""",
                
                # Таблица продуктов
                """CREATE TABLE IF NOT EXISTS retail_nds.products (
                    product_id UInt32,
                    product_line_id String,
                    unit_price Decimal(10,2),
                    PRIMARY KEY (product_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (product_id)""",
                
                # Таблица инвойсов
                """CREATE TABLE IF NOT EXISTS retail_nds.invoices (
                    invoice_id String,
                    branch_id String,
                    customer_id UInt32,
                    date Date,
                    time String,
                    payment_method_id String,
                    rating Nullable(Decimal(3,1)),
                    PRIMARY KEY (invoice_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (invoice_id, date)""",
                
                # Таблица позиций инвойсов
                """CREATE TABLE IF NOT EXISTS retail_nds.invoice_items (
                    item_id UInt32,
                    invoice_id String,
                    product_id UInt32,
                    quantity UInt32,
                    tax_amount Decimal(10,2),
                    total Decimal(10,2),
                    cogs Decimal(10,2),
                    gross_margin_percentage Nullable(Decimal(5,2)),
                    gross_income Decimal(10,2),
                    PRIMARY KEY (item_id)
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (item_id, invoice_id)""",
                
                # Таблица сырых данных (уже была)
                """CREATE TABLE IF NOT EXISTS retail_nds.raw_sales (
                    invoice_id String,
                    branch String,
                    city String,
                    customer_type String,
                    gender String,
                    product_line String,
                    unit_price Float64,
                    quantity Int32,
                    tax_5percent Float64,
                    total Float64,
                    date String,
                    time String,
                    payment String,
                    cogs Float64,
                    gross_margin_percent Float64,
                    gross_income Float64,
                    rating Float32,
                    loaded_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (invoice_id, date)""",
                
                ##############################################
                # Таблицы в DDS (витрины данных)
                ##############################################
                
                # Таблица городов
                """CREATE TABLE IF NOT EXISTS retail_dwh.dim_cities (
                    city_id String,
                    city_name String,
                    timezone String DEFAULT '',
                    _version UInt64 DEFAULT 1,
                    PRIMARY KEY (city_id)
                ) ENGINE = ReplacingMergeTree(_version)
                ORDER BY (city_id)""",
                
                # Таблица филиалов
                """CREATE TABLE IF NOT EXISTS retail_dwh.dim_branches (
                    branch_id String,
                    branch_name String,
                    city_id String DEFAULT '',
                    city_name String,
                    region String DEFAULT '',
                    _version UInt64 DEFAULT 1,
                    PRIMARY KEY (branch_id)
                ) ENGINE = ReplacingMergeTree(_version)
                ORDER BY (branch_id)""",
                
                # Таблица покупателей                
                """CREATE TABLE IF NOT EXISTS retail_dwh.dim_customers (
                    customer_id String,
                    customer_type String,
                    gender String,
                    _version UInt64 DEFAULT 1,
                    PRIMARY KEY (customer_id)
                ) ENGINE = ReplacingMergeTree(_version)
                ORDER BY (customer_id)""",
                
                # Таблица продуктов                   
                """CREATE TABLE IF NOT EXISTS retail_dwh.dim_products (
                    product_id String,
                    product_line String,
                    price_segment String DEFAULT '',
                    _version UInt64 DEFAULT 1,
                    PRIMARY KEY (product_id)
                ) ENGINE = ReplacingMergeTree(_version)
                ORDER BY (product_id)""",
                
                # Таблица методов оплаты 
                """CREATE TABLE IF NOT EXISTS retail_dwh.dim_payment_methods (
                    payment_id String,
                    payment_type String,
                    _version UInt64 DEFAULT 1,
                    PRIMARY KEY (payment_id)
                ) ENGINE = ReplacingMergeTree(_version)
                ORDER BY (payment_id)""",
                
                # Таблица времени и дат 
                """CREATE TABLE IF NOT EXISTS retail_dwh.dim_time (
                    date_id Date,
                    day_of_week Int8,
                    is_weekend UInt8,
                    month Int8,
                    quarter Int8,
                    year Int16,
                    is_holiday UInt8,
                    holiday_name String DEFAULT '',
                    _version UInt64 DEFAULT 1,
                    PRIMARY KEY (date_id)
                ) ENGINE = ReplacingMergeTree(_version)
                ORDER BY (date_id)""",
                
                # Таблица фактов продаж
                """CREATE TABLE IF NOT EXISTS retail_dwh.fact_sales (
                    sale_id UInt64,
                    branch_id String DEFAULT '',
                    customer_id String DEFAULT '',
                    product_id String DEFAULT '',
                    payment_id String DEFAULT '',
                    date_id Date,
                    invoice_id String,
                    quantity UInt32,
                    unit_price Decimal(10,2),
                    tax_amount Decimal(10,2),
                    total_amount Decimal(10,2),
                    cogs Decimal(10,2),
                    gross_margin_percent Nullable(Decimal(5,2)),
                    gross_income Nullable(Decimal(10,2)),
                    rating Nullable(Decimal(3,1)),
                    loaded_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (sale_id, date_id, invoice_id)"""
            ]
            
            for query in init_queries:
                ch_client.execute(query)
            
            logging.info("Базы данных и таблицы успешно инициализированы")
            return True
            
        except Exception as e:
            logging.error(f"Попытка {attempt + 1} не удалась: {str(e)}")
            if attempt == max_retries - 1:
                raise AirflowException(f"Не удалось инициализировать ClickHouse после {max_retries} попыток")
            time.sleep(retry_delay)

def fetch_holidays():
    #Получает данные о праздниках с API с кэшированием
    try:
        # Пробуем загрузить из кэша
        try:
            holidays = pd.read_csv('/opt/airflow/data/holidays_2024_cache.csv')
            logging.info("Данные о праздниках загружены из кэша")
            return holidays
        except Exception as e:
            logging.error(f"Ошибка при загрузке праздников: {str(e)}")
            return pd.DataFrame()  # Возвращаем пустой DataFrame при ошибке
        
        # Если кэша нет, делаем запрос к API
        logging.info("Делаем запрос к HolidayAPI...")
        url = f"https://holidayapi.com/v1/holidays?country={COUNTRY}&year={YEAR}&key={HOLIDAY_API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        holidays_data = response.json().get('holidays', [])
        holidays = pd.DataFrame(holidays_data)
        
        # Сохраняем в кэш
        holidays.to_csv('/opt/airflow/data/holidays_2024_cache.csv', index=False)
        logging.info(f"Получено {len(holidays)} праздников за {YEAR} год")
        return holidays
    
    except Exception as e:
        logging.error(f"Ошибка при получении данных о праздниках: {e}")
        return pd.DataFrame()

def prepare_dim_time(df):
    #Подготавливает таблицу времени с информацией о праздниках
    try:
        if 'date' not in df.columns:
            raise ValueError("Колонка 'date' не найдена в данных")
            
        # Преобразуем дату (уже в формате YYYY-MM-DD из NDS слоя)
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        unique_dates = pd.DataFrame({'date': df['date'].dt.normalize().unique()})
                
        # Извлекаем компоненты даты
        unique_dates['date_id'] = unique_dates['date'].dt.date
        unique_dates['day_of_week'] = unique_dates['date'].dt.dayofweek + 1
        unique_dates['is_weekend'] = unique_dates['day_of_week'].isin([6, 7]).astype(int)
        unique_dates['month'] = unique_dates['date'].dt.month
        unique_dates['quarter'] = unique_dates['date'].dt.quarter
        unique_dates['year'] = unique_dates['date'].dt.year
        
        # Получаем данные о праздниках
        holidays = fetch_holidays()
        
        if not holidays.empty:
            # Обрабатываем праздники
            holidays['date'] = pd.to_datetime(holidays['date'])
            holidays['month_day'] = holidays['date'].dt.strftime('%m-%d')
            
            # Создаём словарь для сопоставления
            holiday_dict = dict(zip(holidays['month_day'], holidays['name']))
            
            # Добавляем информацию о праздниках
            unique_dates['month_day'] = unique_dates['date'].dt.strftime('%m-%d')
            unique_dates['is_holiday'] = unique_dates['month_day'].isin(holiday_dict).astype(int)
            unique_dates['holiday_name'] = unique_dates['month_day'].map(holiday_dict).fillna('')
            unique_dates.drop('month_day', axis=1, inplace=True)
        else:
            unique_dates['is_holiday'] = 0
            unique_dates['holiday_name'] = ''
        
        return unique_dates
    
    except Exception as e:
        logging.error(f"Ошибка подготовки таблицы времени: {str(e)}")
        raise

def load_raw_data_to_nds():
    #Загружает сырые данные в слой NDS
    try:
        # Читаем CSV файл с правильными именами колонок
        df = pd.read_csv(
            '/opt/airflow/data/sales_data.csv',
            dtype={
                'Unit price': 'float64',
                'Tax 5%': 'float64',
                'Total': 'float64',
                'Quantity': 'int32',
                'Rating': 'float32'
            },
            parse_dates=['Date'],  # Автоматически парсим даты при загрузке
            date_parser=lambda x: pd.to_datetime(x, format='%m/%d/%Y')  # Указываем точный формат
        )
        
        # Проверяем обязательные колонки
        required_columns = ['Invoice ID', 'Branch', 'City', 'Customer type', 'Gender', 
                          'Product line', 'Unit price', 'Quantity', 'Tax 5%', 'Total',
                          'Date', 'Time', 'Payment', 'cogs', 'gross margin percentage',
                          'gross income', 'Rating']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Отсутствуют колонки в CSV: {missing_columns}")
        
        # Переименовываем колонки для соответствия таблице NDS
        df = df.rename(columns={
            'Invoice ID': 'invoice_id',
            'Branch': 'branch',
            'City': 'city',
            'Customer type': 'customer_type',
            'Gender': 'gender',
            'Product line': 'product_line',
            'Unit price': 'unit_price',
            'Quantity': 'quantity',
            'Tax 5%': 'tax_5percent',
            'Total': 'total',
            'Date': 'date',
            'Time': 'time',
            'Payment': 'payment',
            'gross margin percentage': 'gross_margin_percent',
            'gross income': 'gross_income',
            'Rating': 'rating'
        })
        
        # Преобразуем дату в строку формата YYYY-MM-DD
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        data_for_insert = df.to_dict('records')
        for row in data_for_insert:
            if 'loaded_at' in row:
                del row['loaded_at']
        
        # Загружаем в NDS
        max_retries = 3
        for attempt in range(max_retries):
            try:
                ch_client = get_clickhouse_client()
                ch_client.execute(
                    "INSERT INTO retail_nds.raw_sales (invoice_id, branch, city, customer_type, gender, "
                    "product_line, unit_price, quantity, tax_5percent, total, date, time, payment, "
                    "cogs, gross_margin_percent, gross_income, rating) VALUES",
                    data_for_insert
                )
                logging.info("Сырые данные успешно загружены в NDS")
                return True
                
            except Exception as e:
                logging.error(f"Попытка {attempt + 1} не удалась: {str(e)}")
                if attempt == max_retries - 1:
                    raise AirflowException(f"Не удалось загрузить данные в NDS после {max_retries} попыток")
                time.sleep(10)
    
    except Exception as e:
        raise AirflowException(f"Ошибка загрузки в NDS: {str(e)}")
        
def load_normalized_data_to_nds():
    #Заполняет нормализованные таблицы NDS из raw_sales
    try:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                ch_client = get_clickhouse_client()
                
                # 1. Заполняем справочники
                
                # Города
                ch_client.execute("""
                    INSERT INTO retail_nds.cities (city_id, city)
                    SELECT 
                        cityHash64(city) as city_id,
                        city
                    FROM retail_nds.raw_sales
                    GROUP BY city
                """)
                
                # Типы клиентов
                ch_client.execute("""
                    INSERT INTO retail_nds.customer_types (customer_type_id, type_name)
                    SELECT 
                        cityHash64(customer_type) as customer_type_id,
                        customer_type as type_name
                    FROM retail_nds.raw_sales
                    GROUP BY customer_type
                """)
                
                # Гендеры
                ch_client.execute("""
                    INSERT INTO retail_nds.genders (gender_id, gender_name)
                    SELECT 
                        cityHash64(gender) as gender_id,
                        gender as gender_name
                    FROM retail_nds.raw_sales
                    GROUP BY gender
                """)
                
                # Линейки продуктов
                ch_client.execute("""
                    INSERT INTO retail_nds.product_lines (product_line_id, line_name)
                    SELECT 
                        cityHash64(product_line) as product_line_id,
                        product_line as line_name
                    FROM retail_nds.raw_sales
                    GROUP BY product_line
                """)
                
                # Методы оплаты
                ch_client.execute("""
                    INSERT INTO retail_nds.payment_methods (payment_method_id, method_name)
                    SELECT 
                        cityHash64(payment) as payment_method_id,
                        payment as method_name
                    FROM retail_nds.raw_sales
                    GROUP BY payment
                """)
                
                # 2. Заполняем основные таблицы
                
                # Филиалы
                ch_client.execute("""
                    INSERT INTO retail_nds.branches (branch_id, branch_name, city_id)
                    SELECT 
                        cityHash64(branch) as branch_id,
                        branch as branch_name,
                        cityHash64(city) as city_id
                    FROM retail_nds.raw_sales
                    GROUP BY branch, city
                """)
                
                # Клиенты
                ch_client.execute("""
                    INSERT INTO retail_nds.customers (customer_id, customer_type_id, gender_id)
                    SELECT 
                        cityHash64(concat(customer_type, gender)) as customer_id,
                        cityHash64(customer_type) as customer_type_id,
                        cityHash64(gender) as gender_id
                    FROM retail_nds.raw_sales
                    GROUP BY customer_type, gender
                """)
                
                # Продукты
                ch_client.execute("""
                    INSERT INTO retail_nds.products (product_id, product_line_id, unit_price)
                    SELECT 
                        cityHash64(product_line) as product_id,
                        cityHash64(product_line) as product_line_id,
                        avg(unit_price) as unit_price
                    FROM retail_nds.raw_sales
                    GROUP BY product_line
                """)
                
                # Инвойсы
                ch_client.execute("""
                    INSERT INTO retail_nds.invoices (
                        invoice_id, branch_id, customer_id, date, time, 
                        payment_method_id, rating
                    )
                    SELECT 
                        invoice_id,
                        cityHash64(branch) as branch_id,
                        cityHash64(concat(customer_type, gender)) as customer_id,
                        toDate(date) as date,
                        time,
                        cityHash64(payment) as payment_method_id,
                        rating
                    FROM retail_nds.raw_sales
                """)
                
                # Позиции инвойсов
                ch_client.execute("""
                    INSERT INTO retail_nds.invoice_items (
                        item_id, invoice_id, product_id, quantity, tax_amount, 
                        total, cogs, gross_margin_percentage, gross_income
                    )
                    SELECT 
                        rowNumberInAllBlocks() as item_id,
                        invoice_id,
                        cityHash64(product_line) as product_id,
                        quantity,
                        tax_5percent as tax_amount,
                        total,
                        cogs,
                        gross_margin_percent as gross_margin_percentage,
                        gross_income
                    FROM retail_nds.raw_sales
                """)
                
                logging.info("Нормализованные данные успешно загружены в NDS")
                return True
                
            except Exception as e:
                logging.error(f"Попытка {attempt + 1} не удалась: {str(e)}")
                if attempt == max_retries - 1:
                    raise AirflowException(f"Не удалось загрузить нормализованные данные в NDS после {max_retries} попыток")
                time.sleep(10)
    
    except Exception as e:
        raise AirflowException(f"Ошибка загрузки нормализованных данных в NDS: {str(e)}")


def transform_and_load_to_dds():
    #Преобразует данные из NDS и загружает в DDS
    try:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                ch_client = get_clickhouse_client()
                
                # 1. Получаем сырые данные из NDS
                raw_data = ch_client.query_dataframe(
                    "SELECT * FROM retail_nds.raw_sales"
                )
                
                if raw_data.empty:
                    raise ValueError("Нет данных в NDS для обработки")
                
                logging.info(f"Колонки в raw_data: {raw_data.columns.tolist()}")
                
                # 2. Подготавливаем таблицу времени
                dim_time_df = prepare_dim_time(raw_data)
                
                # 3. Генерируем ID для измерений
                raw_data['branch_id'] = raw_data['branch'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
                raw_data['city_id'] = raw_data['city'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
                raw_data['customer_id'] = raw_data.apply(
                    lambda x: hashlib.md5((str(x['customer_type']) + str(x['gender'])).encode()).hexdigest(), 
                    axis=1
                )
                raw_data['product_id'] = raw_data['product_line'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
                raw_data['payment_id'] = raw_data['payment'].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
                
                # 4. Подготавливаем таблицы измерений
                dim_cities = raw_data[['city_id', 'city']].drop_duplicates()
                dim_cities['timezone'] = ''
                
                dim_branches = raw_data[['branch_id', 'branch', 'city_id', 'city']].drop_duplicates()
                dim_branches['region'] = ''
                
                dim_customers = raw_data[['customer_id', 'customer_type', 'gender']].drop_duplicates()
                
                dim_products = raw_data[['product_id', 'product_line']].drop_duplicates()
                dim_products['price_segment'] = pd.cut(
                    raw_data['unit_price'],
                    bins=3,
                    labels=['low', 'medium', 'high']
                ).fillna('medium')
                
                dim_payments = raw_data[['payment_id', 'payment']].drop_duplicates()
                
                # 5. Подготавливаем факты
                fact_sales = raw_data.rename(columns={
                    'tax_5percent': 'tax_amount',
                    'total': 'total_amount'
                }).copy()
                fact_sales['date_id'] = pd.to_datetime(fact_sales['date'], format='%Y-%m-%d').dt.date
                fact_sales['sale_id'] = range(1, len(fact_sales) + 1)
                
                logging.info(f"Колонки в fact_sales: {fact_sales.columns.tolist()}")
                
                # 6. Загружаем в DDS
                # Очищаем таблицы
                ch_client.execute("TRUNCATE TABLE IF EXISTS retail_dwh.dim_cities")
                ch_client.execute("TRUNCATE TABLE IF EXISTS retail_dwh.dim_branches")
                ch_client.execute("TRUNCATE TABLE IF EXISTS retail_dwh.dim_customers")
                ch_client.execute("TRUNCATE TABLE IF EXISTS retail_dwh.dim_products")
                ch_client.execute("TRUNCATE TABLE IF EXISTS retail_dwh.dim_payment_methods")
                ch_client.execute("TRUNCATE TABLE IF EXISTS retail_dwh.dim_time")
                
                # Вставляем данные
                dim_tables = [
                    ('dim_time', dim_time_df),
                    ('dim_cities', dim_cities.rename(columns={'city': 'city_name'})),
                    ('dim_branches', dim_branches.rename(columns={
                        'branch': 'branch_name',
                        'city': 'city_name'
                    })),
                    ('dim_customers', dim_customers.rename(columns={
                        'customer_type': 'customer_type',
                        'gender': 'gender'
                    })),
                    ('dim_products', dim_products.rename(columns={'product_line': 'product_line'})),
                    ('dim_payment_methods', dim_payments.rename(columns={'payment': 'payment_type'}))
                ]
                
                for table_name, data in dim_tables:
                    ch_client.execute(f"INSERT INTO retail_dwh.{table_name} VALUES", data.to_dict('records'))
                
                # Вставляем факты
                ch_client.execute(
                    """INSERT INTO retail_dwh.fact_sales (
                        sale_id, branch_id, customer_id, product_id, 
                        payment_id, date_id, invoice_id, quantity, 
                        unit_price, tax_amount, total_amount, cogs, 
                        gross_margin_percent, gross_income, rating
                    ) VALUES""",
                    fact_sales[[
                        'sale_id', 'branch_id', 'customer_id', 'product_id', 'payment_id',
                        'date_id', 'invoice_id', 'quantity', 'unit_price', 'tax_amount',
                        'total_amount', 'cogs', 'gross_margin_percent', 'gross_income', 'rating'
                    ]].to_dict('records')
                )
                
                logging.info("Данные успешно загружены в DDS")
                return True
                
            except Exception as e:
                logging.error(f"Попытка {attempt + 1} не удалась: {str(e)}")
                if attempt == max_retries - 1:
                    raise AirflowException(f"Не удалось загрузить данные в DDS после {max_retries} попыток")
                time.sleep(10)
    
    except Exception as e:
        raise AirflowException(f"Ошибка преобразования и загрузки в DDS: {str(e)}")

with DAG(
    'clickhouse_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['clickhouse', 'etl'],
) as dag:
    
    init_db_task = PythonOperator(
        task_id='init_clickhouse_db',
        python_callable=init_clickhouse_db,
        retries=0,
    )
    
    load_nds_raw_task = PythonOperator(
        task_id='load_raw_data_to_nds',
        python_callable=load_raw_data_to_nds,
        retries=0,
    )
    
    load_nds_normalized_task = PythonOperator(
        task_id='load_normalized_data_to_nds',
        python_callable=load_normalized_data_to_nds,
        retries=0,
    )
    
    transform_load_dds_task = PythonOperator(
        task_id='transform_and_load_to_dds',
        python_callable=transform_and_load_to_dds,
        retries=0,
    )
    
    init_db_task >> load_nds_raw_task >> load_nds_normalized_task >> transform_load_dds_task