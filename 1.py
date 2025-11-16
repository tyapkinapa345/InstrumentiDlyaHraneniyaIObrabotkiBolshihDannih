# Установка необходимых библиотек
!pip install pandas numpy pymongo psycopg2-binary sqlalchemy matplotlib seaborn faker

# Импорт необходимых библиотек
import pandas as pd
import numpy as np
from pymongo import MongoClient
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import time
import warnings
from faker import Faker
import random

warnings.filterwarnings('ignore')

# Настройка для отображения графиков
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Инициализация Faker
fake = Faker()
Faker.seed(42)
np.random.seed(42)

# Функции для проверки подключения к базам данных
def check_mongo_connection(client):
    """Проверка подключения к MongoDB"""
    try:
        client.server_info()
        print("✅ Успешное подключение к MongoDB")
        return True
    except Exception as e:
        print(f"❌ Ошибка подключения к MongoDB: {e}")
        return False

def check_postgres_connection(conn_params):
    """Проверка подключения к PostgreSQL"""
    try:
        conn = psycopg2.connect(**conn_params)
        print("✅ Успешное подключение к PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        return None

def measure_time(func, *args, **kwargs):
    """Измерение времени выполнения функции"""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

# Генерация IoT данных (адаптированная версия функции преподавателя)
def generate_iot_data(n_records, n_devices=100):
    """Генерация IoT данных для сенсоров"""
    iot_data = []
    
    # Создаем список device_id для обеспечения реалистичного распределения
    device_ids = [f"device_{i:03d}" for i in range(n_devices)]
    
    # Добавляем несколько "особых" устройств для анализа
    special_devices = ["sensor_alpha", "sensor_beta", "sensor_gamma"]
    device_ids.extend(special_devices)
    
    start_date = datetime(2024, 1, 1)
    
    for i in range(n_records):
        # Выбираем device_id с учетом распределения (некоторые устройства более активны)
        device_id = np.random.choice(device_ids, p=np.random.dirichlet(np.ones(len(device_ids))))
        
        # Генерируем timestamp в пределах года
        timestamp = start_date + timedelta(
            days=np.random.randint(0, 365),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        )
        
        # Генерируем данные сенсора с некоторой корреляцией
        base_temp = np.random.normal(20, 10)  # Базовая температура
        
        # Сезонные колебания
        day_of_year = timestamp.timetuple().tm_yday
        seasonal_effect = 10 * np.sin(2 * np.pi * day_of_year / 365)
        
        # Суточные колебания
        hour_effect = 5 * np.sin(2 * np.pi * timestamp.hour / 24)
        
        # Финальная температура с эффектами
        temperature = round(base_temp + seasonal_effect + hour_effect + np.random.normal(0, 2), 1)
        
        # Ограничиваем диапазон температуры
        temperature = max(-20, min(60, temperature))
        
        data = {
            "sensor_id": device_id,
            "temperature": temperature,
            "timestamp": timestamp,
            "humidity": round(random.uniform(0, 100), 1),
            "pressure": round(random.uniform(900, 1100), 1),
            "battery_level": random.randint(0, 100),
            "record_id": i
        }
        iot_data.append(data)
    
    return iot_data

# Параметры данных
n_records = 100000  # 100,000 записей IoT данных
n_devices = 103     # 100 обычных + 3 специальных устройства

print("🔧 Генерация IoT данных...")
print(f"- Записей: {n_records:,}")
print(f"- Устройств: {n_devices}")

# Генерация данных
iot_data = generate_iot_data(n_records, n_devices)

# Создание DataFrame
iot_df = pd.DataFrame(iot_data)

print(f"\n✅ Сгенерирован DataFrame с IoT данными:")
print(f"- Записей: {len(iot_df):,}")
print(f"- Уникальных сенсоров: {iot_df['sensor_id'].nunique()}")

# Показываем информацию о данных
print("\n📊 Информация о данных:")
print(iot_df.info())
print("\n📈 Статистика температуры:")
print(iot_df['temperature'].describe())

# Визуализация распределения температуры
plt.figure(figsize=(12, 6))

plt.subplot(1, 2, 1)
plt.hist(iot_df['temperature'], bins=50, alpha=0.7, color='skyblue', edgecolor='black')
plt.title('Распределение температуры')
plt.xlabel('Температура (°C)')
plt.ylabel('Частота')

plt.subplot(1, 2, 2)
top_devices = iot_df['sensor_id'].value_counts().head(10)
plt.bar(range(len(top_devices)), top_devices.values, color='lightcoral', alpha=0.7)
plt.title('Топ-10 самых активных сенсоров')
plt.xlabel('Сенсор')
plt.ylabel('Количество записей')
plt.xticks(range(len(top_devices)), [f"Device {i+1}" for i in range(len(top_devices))], rotation=45)

plt.tight_layout()
plt.show()

# Сохранение данных в CSV файл
iot_df.to_csv('iot_sensor_data.csv', index=False)
print("✅ Данные сохранены в 'iot_sensor_data.csv'")

## 2. Подключение к MongoDB и загрузка данных

print("\n" + "="*50)
print("📊 MONGODB: ЗАГРУЗКА И АНАЛИЗ ДАННЫХ")
print("="*50)

# Подключение к MongoDB
try:
    # Попробуем подключиться к MongoDB через имя сервиса (для Docker)
    mongo_client = MongoClient('mongodb://mongouser:mongopass@mongodb:27017/')
    if check_mongo_connection(mongo_client):
        print("✅ Подключение через Docker сервис 'mongodb'")
    else:
        raise Exception("Не удалось подключиться через Docker сервис")
except:
    try:
        # Если не работает через Docker, попробуем localhost
        mongo_client = MongoClient('mongodb://mongouser:mongopass@localhost:27017/')
        if check_mongo_connection(mongo_client):
            print("✅ Подключение через localhost")
        else:
            raise Exception("Не удалось подключиться через localhost")
    except:
        print("❌ Не удалось подключиться к MongoDB")
        print("Проверьте, что MongoDB запущен: docker compose ps")
        mongo_client = None

if mongo_client:
    mongo_db = mongo_client['iot_studies']
    
    # Очистка существующей коллекции
    mongo_db.sensor_data.drop()
    
    # Загрузка данных в MongoDB
    print("📥 Загрузка IoT данных в MongoDB...")
    
    sensor_collection = mongo_db['sensor_data']
    sensor_records = iot_df.to_dict('records')
    sensor_collection.insert_many(sensor_records)
    print(f"✅ Загружено {len(sensor_records):,} записей в коллекцию sensor_data")
    
    # Создание индексов для оптимизации
    sensor_collection.create_index("sensor_id")
    sensor_collection.create_index("timestamp")
    sensor_collection.create_index([("sensor_id", 1), ("timestamp", 1)])
    print("✅ Созданы индексы для оптимизации запросов")
    
    # ВЫПОЛНЕНИЕ ЗАДАНИЯ: Агрегационный запрос для поиска максимальной температуры для каждого сенсора
    print("\n🔍 ВЫПОЛНЕНИЕ ЗАДАНИЯ: Поиск максимальной температуры для каждого сенсора")
    
    def mongodb_max_temperature_query():
        """Агрегационный запрос MongoDB для поиска максимальной температуры по сенсорам"""
        pipeline = [
            {
                "$group": {
                    "_id": "$sensor_id",
                    "max_temperature": {"$max": "$temperature"},
                    "total_records": {"$sum": 1}
                }
            },
            {
                "$sort": {"max_temperature": -1}
            }
        ]
        return list(sensor_collection.aggregate(pipeline))
    
    # Измеряем время выполнения
    mongo_result, mongo_time = measure_time(mongodb_max_temperature_query)
    
    print(f"⏱️ Время выполнения MongoDB агрегации: {mongo_time:.4f} секунд")
    print(f"📊 Найдено {len(mongo_result)} уникальных сенсоров")
    
    # Показываем топ-5 сенсоров с самой высокой температурой
    print("\n🔥 Топ-5 сенсоров с максимальной температурой (MongoDB):")
    for i, sensor in enumerate(mongo_result[:5]):
        print(f"  {i+1}. {sensor['_id']}: {sensor['max_temperature']}°C (записей: {sensor['total_records']})")
    
else:
    print("❌ Пропуск операций с MongoDB из-за ошибки подключения")
    mongo_time = None

## 3. Подключение к PostgreSQL и загрузка данных

print("\n" + "="*50)
print("📊 POSTGRESQL: ЗАГРУЗКА И АНАЛИЗ ДАННЫХ")
print("="*50)

# Подключение к PostgreSQL
pg_conn_params = {
    "dbname": "studpg",
    "user": "postgres",
    "password": "changeme",
    "host": "postgresql",  # Имя сервиса в docker-compose
    "port": "5432"
}

pg_conn = check_postgres_connection(pg_conn_params)
if pg_conn:
    try:
        # Создание таблицы sensor_data
        with pg_conn.cursor() as cur:
            # Удаление существующей таблицы
            cur.execute("DROP TABLE IF EXISTS sensor_data CASCADE")
            
            # Создание таблицы sensor_data согласно заданию
            cur.execute("""
                CREATE TABLE sensor_data (
                    record_id INTEGER PRIMARY KEY,
                    sensor_id VARCHAR(50) NOT NULL,
                    temperature DECIMAL(5,2) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    humidity DECIMAL(5,2),
                    pressure DECIMAL(6,2),
                    battery_level INTEGER
                )
            """)
            
            # Создание индексов для оптимизации
            cur.execute("CREATE INDEX idx_sensor_data_sensor_id ON sensor_data(sensor_id)")
            cur.execute("CREATE INDEX idx_sensor_data_timestamp ON sensor_data(timestamp)")
            cur.execute("CREATE INDEX idx_sensor_data_temperature ON sensor_data(temperature)")
        
        print("✅ Создана таблица sensor_data и индексы")
        
        # Загрузка данных
        print("📥 Загрузка IoT данных в PostgreSQL...")
        
        with pg_conn.cursor() as cur:
            for _, row in iot_df.iterrows():
                cur.execute("""
                    INSERT INTO sensor_data (record_id, sensor_id, temperature, timestamp, humidity, pressure, battery_level)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['record_id'], row['sensor_id'], row['temperature'], 
                    row['timestamp'], row['humidity'], row['pressure'], row['battery_level']
                ))
        
        pg_conn.commit()
        print(f"✅ Загружено {len(iot_df):,} записей в таблицу sensor_data")

        # ВЫПОЛНЕНИЕ ЗАДАНИЯ: Найти максимальную температуру для каждого сенсора за все время
        print("\n🔍 ВЫПОЛНЕНИЕ ЗАДАНИЯ: Поиск максимальной температуры для каждого сенсора")
        
        def postgres_max_temperature_query():
            """SQL запрос для поиска максимальной температуры по сенсорам"""
            with pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        sensor_id,
                        MAX(temperature) as max_temperature,
                        COUNT(*) as total_records
                    FROM sensor_data
                    GROUP BY sensor_id
                    ORDER BY max_temperature DESC
                """)
                return cur.fetchall()
        
        # Измеряем время выполнения
        pg_result, pg_time = measure_time(postgres_max_temperature_query)
        
        print(f"⏱️ Время выполнения PostgreSQL запроса: {pg_time:.4f} секунд")
        print(f"📊 Найдено {len(pg_result)} уникальных сенсоров")
        
        # Показываем топ-5 сенсоров с самой высокой температурой
        print("\n🔥 Топ-5 сенсоров с максимальной температурой (PostgreSQL):")
        for i, (sensor_id, max_temp, count) in enumerate(pg_result[:5]):
            print(f"  {i+1}. {sensor_id}: {max_temp}°C (записей: {count})")

    except Exception as e:
        print(f"❌ Ошибка при работе с PostgreSQL: {e}")
        pg_time = None
    finally:
        pg_conn.close()
else:
    print("❌ Пропуск операций с PostgreSQL из-за ошибки подключения")
    pg_time = None

## 4. Сравнение производительности и анализ

print("\n" + "="*50)
print("📊 СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
print("="*50)

if mongo_time is not None and pg_time is not None:
    # Создаем DataFrame для сравнения
    comparison_data = {
        'Database': ['MongoDB', 'PostgreSQL'],
        'Query_Time_Seconds': [mongo_time, pg_time],
        'Records_Processed': [n_records, n_records],
        'Query_Type': ['Aggregation Pipeline', 'SQL GROUP BY']
    }
    
    comparison_df = pd.DataFrame(comparison_data)
    
    # Визуализация сравнения производительности
    plt.figure(figsize=(10, 6))
    
    bars = plt.bar(comparison_df['Database'], comparison_df['Query_Time_Seconds'], 
                   color=['#4CAF50', '#2196F3'], alpha=0.7, edgecolor='black')
    
    plt.title('Сравнение производительности: Максимальная температура по сенсорам', fontsize=14, fontweight='bold')
    plt.ylabel('Время выполнения (секунды)', fontsize=12)
    plt.xlabel('База данных', fontsize=12)
    
    # Добавляем значения на столбцы
    for bar, time_val in zip(bars, comparison_df['Query_Time_Seconds']):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                f'{time_val:.4f}s', ha='center', va='bottom', fontweight='bold')
    
    # Вычисляем разницу в производительности
    faster_db = 'MongoDB' if mongo_time < pg_time else 'PostgreSQL'
    time_diff = abs(mongo_time - pg_time)
    faster_percent = (time_diff / min(mongo_time, pg_time)) * 100
    
    plt.figtext(0.5, 0.01, 
               f"🎯 {faster_db} быстрее на {faster_percent:.1f}% ({time_diff:.4f} секунд)", 
               ha="center", fontsize=12, bbox={"facecolor":"orange", "alpha":0.2, "pad":5})
    
    plt.tight_layout()
    plt.show()
    
    print("\n📋 РЕЗУЛЬТАТЫ СРАВНЕНИЯ:")
    print(f"   MongoDB Aggregation:     {mongo_time:.4f} секунд")
    print(f"   PostgreSQL GROUP BY:     {pg_time:.4f} секунд")
    print(f"   Разница:                 {abs(mongo_time - pg_time):.4f} секунд")
    print(f"   Победитель:              {faster_db}")
    
    # Анализ результатов
    print("\n🔍 АНАЛИЗ:")
    if faster_db == 'MongoDB':
        print("   • MongoDB показала лучшую производительность для агрегационных операций")
        print("   • Агрегационный pipeline оптимизирован для обработки документов")
    else:
        print("   • PostgreSQL показала лучшую производительность для аналитических запросов")
        print("   • SQL GROUP BY оптимизирован для реляционных операций")
    
    print("   • Обе СУБД эффективно справились с обработкой 100,000+ записей")
    
else:
    print("❌ Невозможно выполнить сравнение: отсутствуют данные о времени выполнения")

## 5. Дополнительный анализ данных

print("\n" + "="*50)
print("📊 ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ ДАННЫХ")
print("="*50)

# Анализ распределения данных по сенсорам
device_stats = iot_df.groupby('sensor_id').agg({
    'temperature': ['count', 'min', 'max', 'mean', 'std'],
    'humidity': 'mean',
    'battery_level': 'mean'
}).round(2)

device_stats.columns = ['records', 'min_temp', 'max_temp', 'avg_temp', 'std_temp', 'avg_humidity', 'avg_battery']
device_stats = device_stats.sort_values('records', ascending=False)

print("📈 Статистика по сенсорам (топ-10 по количеству записей):")
print(device_stats.head(10))

# Визуализация временных рядов для нескольких сенсоров
plt.figure(figsize=(15, 10))

# Выбираем 5 случайных сенсоров для визуализации
sample_sensors = np.random.choice(iot_df['sensor_id'].unique(), 5, replace=False)

for i, sensor_id in enumerate(sample_sensors, 1):
    plt.subplot(3, 2, i)
    sensor_data = iot_df[iot_df['sensor_id'] == sensor_id].sort_values('timestamp')
    
    plt.plot(sensor_data['timestamp'], sensor_data['temperature'], 
             marker='o', markersize=2, linewidth=1, alpha=0.7)
    plt.title(f'Сенсор {sensor_id}\nМакс темп: {sensor_data["temperature"].max()}°C')
    plt.xlabel('Время')
    plt.ylabel('Температура (°C)')
    plt.xticks(rotation=45)

plt.suptitle('Временные ряды температуры для различных сенсоров', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

print("\n✅ Анализ завершен! Код готов для выполнения в Jupyter Notebook")
