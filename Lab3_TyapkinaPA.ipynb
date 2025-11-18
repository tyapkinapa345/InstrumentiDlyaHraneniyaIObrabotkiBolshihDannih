```python
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
import random

warnings.filterwarnings('ignore')

# Настройка для отображения графиков
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

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

# Генерация IoT данных
def generate_iot_data(n_records, n_devices=100):
    """Генерация IoT данных для сенсоров"""
    iot_data = []
    
    device_ids = [f"device_{i:03d}" for i in range(n_devices)]
    special_devices = ["sensor_alpha", "sensor_beta", "sensor_gamma"]
    device_ids.extend(special_devices)
    
    start_date = datetime(2024, 1, 1)
    
    for i in range(n_records):
        device_id = np.random.choice(device_ids, p=np.random.dirichlet(np.ones(len(device_ids))))
        
        timestamp = start_date + timedelta(
            days=np.random.randint(0, 365),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        )
        
        base_temp = np.random.normal(20, 10)
        day_of_year = timestamp.timetuple().tm_yday
        seasonal_effect = 10 * np.sin(2 * np.pi * day_of_year / 365)
        hour_effect = 5 * np.sin(2 * np.pi * timestamp.hour / 24)
        temperature = round(base_temp + seasonal_effect + hour_effect + np.random.normal(0, 2), 1)
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
n_records = 1000000
n_devices = 103

print("🔧 Генерация IoT данных...")
print(f"- Записей: {n_records:,}")
print(f"- Устройств: {n_devices}")

# Генерация данных
iot_data = generate_iot_data(n_records, n_devices)
iot_df = pd.DataFrame(iot_data)

print(f"\n✅ Сгенерирован DataFrame с IoT данными:")
print(f"- Записей: {len(iot_df):,}")
print(f"- Уникальных сенсоров: {iot_df['sensor_id'].nunique()}")

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

## Подключение к MongoDB и загрузка данных
print("\n" + "="*50)
print("📊 MONGODB: ЗАГРУЗКА И АНАЛИЗ ДАННЫХ")
print("="*50)

# Подключение к MongoDB
try:
    mongo_client = MongoClient('mongodb://mongouser:mongopass@mongodb:27017/')
    if check_mongo_connection(mongo_client):
        print("✅ Подключение через Docker сервис 'mongodb'")
    else:
        raise Exception("Не удалось подключиться через Docker сервис")
except:
    try:
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
    mongo_db.sensor_data.drop()
    
    print("📥 Загрузка IoT данных в MongoDB...")
    sensor_collection = mongo_db['sensor_data']
    sensor_records = iot_df.to_dict('records')
    sensor_collection.insert_many(sensor_records)
    print(f"✅ Загружено {len(sensor_records):,} записей в коллекцию sensor_data")
    
    sensor_collection.create_index("sensor_id")
    sensor_collection.create_index("timestamp")
    sensor_collection.create_index([("sensor_id", 1), ("timestamp", 1)])
    print("✅ Созданы индексы для оптимизации запросов")
    
    # Агрегационный запрос для поиска максимальной температуры
    print("\n🔍 ВЫПОЛНЕНИЕ ЗАДАНИЯ: Поиск максимальной температуры для каждого сенсора")
    
    def mongodb_max_temperature_query():
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
    
    mongo_result, mongo_time = measure_time(mongodb_max_temperature_query)
    
    print(f"⏱️ Время выполнения MongoDB агрегации: {mongo_time:.4f} секунд")
    print(f"📊 Найдено {len(mongo_result)} уникальных сенсоров")
    
    print("\n🔥 Топ-5 сенсоров с максимальной температурой (MongoDB):")
    for i, sensor in enumerate(mongo_result[:5]):
        print(f"  {i+1}. {sensor['_id']}: {sensor['max_temperature']}°C (записей: {sensor['total_records']})")
    
else:
    print("❌ Пропуск операций с MongoDB из-за ошибки подключения")
    mongo_time = None

## Подключение к PostgreSQL и загрузка данных
print("\n" + "="*50)
print("📊 POSTGRESQL: ЗАГРУЗКА И АНАЛИЗ ДАННЫХ")
print("="*50)

# Подключение к PostgreSQL
pg_conn_params = {
    "dbname": "studpg",
    "user": "postgres",
    "password": "changeme",
    "host": "postgresql",
    "port": "5432"
}

pg_conn = check_postgres_connection(pg_conn_params)
if pg_conn:
    try:
        with pg_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS sensor_data CASCADE")
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
            cur.execute("CREATE INDEX idx_sensor_data_sensor_id ON sensor_data(sensor_id)")
            cur.execute("CREATE INDEX idx_sensor_data_timestamp ON sensor_data(timestamp)")
            cur.execute("CREATE INDEX idx_sensor_data_temperature ON sensor_data(temperature)")
        
        print("✅ Создана таблица sensor_data и индексы")
        
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

        # SQL запрос для поиска максимальной температуры
        print("\n🔍 ВЫПОЛНЕНИЕ ЗАДАНИЯ: Поиск максимальной температуры для каждого сенсора")
        
        def postgres_max_temperature_query():
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
        
        pg_result, pg_time = measure_time(postgres_max_temperature_query)
        
        print(f"⏱️ Время выполнения PostgreSQL запроса: {pg_time:.4f} секунд")
        print(f"📊 Найдено {len(pg_result)} уникальных сенсоров")
        
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

## Сравнение производительности
print("\n" + "="*50)
print("📊 СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
print("="*50)

if mongo_time is not None and pg_time is not None:
    comparison_data = {
        'Database': ['MongoDB', 'PostgreSQL'],
        'Query_Time_Seconds': [mongo_time, pg_time],
        'Records_Processed': [n_records, n_records],
        'Query_Type': ['Aggregation Pipeline', 'SQL GROUP BY']
    }
    
    comparison_df = pd.DataFrame(comparison_data)
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(comparison_df['Database'], comparison_df['Query_Time_Seconds'], 
                   color=['#4CAF50', '#2196F3'], alpha=0.7, edgecolor='black')
    
    plt.title('Сравнение производительности: Максимальная температура по сенсорам', fontsize=14, fontweight='bold')
    plt.ylabel('Время выполнения (секунды)', fontsize=12)
    plt.xlabel('База данных', fontsize=12)
    
    for bar, time_val in zip(bars, comparison_df['Query_Time_Seconds']):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                f'{time_val:.4f}s', ha='center', va='bottom', fontweight='bold')
    
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
    
    print("\n🔍 АНАЛИЗ:")
    if faster_db == 'MongoDB':
        print("   • MongoDB показала лучшую производительность для агрегационных операций")
        print("   • Агрегационный pipeline оптимизирован для обработки документов")
    else:
        print("   • PostgreSQL показала лучшую производительность для аналитических запросов")
        print("   • SQL GROUP BY оптимизирован для реляционных операций")
    
    print("   • Обе СУБД эффективно справились с обработкой 1,000,000+ записей")
    
else:
    print("❌ Невозможно выполнить сравнение: отсутствуют данные о времени выполнения")

## Дополнительный анализ данных
print("\n" + "="*50)
print("📊 ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ ДАННЫХ")
print("="*50)

device_stats = iot_df.groupby('sensor_id').agg({
    'temperature': ['count', 'min', 'max', 'mean', 'std'],
    'humidity': 'mean',
    'battery_level': 'mean'
}).round(2)

device_stats.columns = ['records', 'min_temp', 'max_temp', 'avg_temp', 'std_temp', 'avg_humidity', 'avg_battery']
device_stats = device_stats.sort_values('records', ascending=False)

print("📈 Статистика по сенсорам (топ-10 по количеству записей):")
print(device_stats.head(10))

plt.figure(figsize=(15, 10))
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

## Система анализа IoT данных
print("\n" + "="*50)
print("🔍 СИСТЕМА АНАЛИЗА IoT ДАННЫХ")
print("="*50)

# PostgreSQL анализ сенсора
def get_postgres_sensor_analysis(sensor_id, days=30):
    pg_conn = psycopg2.connect(**pg_conn_params)
    try:
        with pg_conn.cursor() as cur:
            query = """
            WITH sensor_stats AS (
                SELECT 
                    sensor_id,
                    AVG(temperature) as avg_temp,
                    STDDEV(temperature) as std_temp,
                    MIN(temperature) as min_temp,
                    MAX(temperature) as max_temp,
                    COUNT(*) as readings_count
                FROM sensor_data 
                WHERE sensor_id = %s 
                AND timestamp >= NOW() - INTERVAL '%s days'
                GROUP BY sensor_id
            )
            SELECT * FROM sensor_stats
            """
            cur.execute(query, (sensor_id, days))
            return cur.fetchall()
    except Exception as e:
        print(f"❌ Ошибка в PostgreSQL запросе анализа сенсора: {e}")
        return []
    finally:
        pg_conn.close()

# MongoDB анализ сенсора
def get_mongodb_sensor_analysis(sensor_id, days=30):
    if not mongo_client:
        return []
    mongo_db = mongo_client['iot_studies']
    sensor_collection = mongo_db['sensor_data']
    
    pipeline = [
        {
            "$match": {
                "sensor_id": sensor_id,
                "timestamp": {"$gte": datetime.now() - timedelta(days=days)}
            }
        },
        {
            "$group": {
                "_id": "$sensor_id",
                "avg_temp": {"$avg": "$temperature"},
                "std_temp": {"$stdDevPop": "$temperature"},
                "min_temp": {"$min": "$temperature"},
                "max_temp": {"$max": "$temperature"},
                "readings_count": {"$sum": 1}
            }
        }
    ]
    return list(sensor_collection.aggregate(pipeline))

# Тестирование анализа
target_sensor = "device_001"
print(f"🎯 Анализ сенсора {target_sensor}:")

postgres_analysis, postgres_analysis_time = measure_time(get_postgres_sensor_analysis, target_sensor, 30)
if postgres_analysis:
    print(f"⏱️ PostgreSQL время выполнения: {postgres_analysis_time:.4f} секунд")
    for sensor_id, avg_temp, std_temp, min_temp, max_temp, count in postgres_analysis:
        print(f"📊 PostgreSQL статистика сенсора {sensor_id}:")
        print(f"  • Средняя температура: {avg_temp:.2f}°C")
        print(f"  • Стандартное отклонение: {std_temp:.2f}°C")

mongodb_analysis, mongodb_analysis_time = measure_time(get_mongodb_sensor_analysis, target_sensor, 30)
if mongodb_analysis:
    print(f"⏱️ MongoDB время выполнения: {mongodb_analysis_time:.4f} секунд")
    for result in mongodb_analysis:
        print(f"📊 MongoDB статистика сенсора {result['_id']}:")
        print(f"  • Средняя температура: {result['avg_temp']:.2f}°C")
        print(f"  • Стандартное отклонение: {result['std_temp']:.2f}°C")

## Сравнение производительности систем анализа
print("\n" + "="*50)
print("📊 СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ IoT АНАЛИЗА")
print("="*50)

if mongo_client:
    test_sensors = ["device_001", "device_050", "sensor_alpha", "device_100", "sensor_beta"]
    postgres_times = []
    mongodb_times = []

    print(f"\n🧪 Тестирование производительности на {len(test_sensors)} сенсорах:")
    
    for sensor_id in test_sensors:
        print(f"\n🔧 Тестирование сенсора {sensor_id}:")
        
        _, pg_time = measure_time(get_postgres_sensor_analysis, sensor_id, 30)
        postgres_times.append(pg_time)
        print(f"  PostgreSQL: {pg_time:.4f} сек")
        
        _, mongo_time = measure_time(get_mongodb_sensor_analysis, sensor_id, 30)
        mongodb_times.append(mongo_time)
        print(f"  MongoDB:    {mongo_time:.4f} сек")
        
        if pg_time < mongo_time:
            faster = "PostgreSQL"
            speedup = mongo_time / pg_time
        else:
            faster = "MongoDB"
            speedup = pg_time / mongo_time
        
        print(f"  🏆 Быстрее: {faster} (в {speedup:.2f} раз)")

    # Визуализация результатов
    plt.figure(figsize=(12, 8))
    
    plt.subplot(2, 2, 1)
    x_pos = np.arange(len(test_sensors))
    width = 0.35
    
    plt.bar(x_pos - width/2, postgres_times, width, label='PostgreSQL', color='blue', alpha=0.7)
    plt.bar(x_pos + width/2, mongodb_times, width, label='MongoDB', color='orange', alpha=0.7)
    plt.xlabel('Сенсоры')
    plt.ylabel('Время выполнения (секунды)')
    plt.title('Время выполнения анализа сенсоров')
    plt.xticks(x_pos, test_sensors, rotation=45)
    plt.legend()
    plt.grid(True, alpha=0.3)

    plt.subplot(2, 2, 2)
    categories = ['PostgreSQL', 'MongoDB']
    avg_times = [np.mean(postgres_times), np.mean(mongodb_times)]
    colors = ['blue', 'orange']
    bars = plt.bar(categories, avg_times, color=colors, alpha=0.7)
    plt.ylabel('Среднее время (секунды)')
    plt.title('Средняя производительность анализа')
    
    for bar, time_val in zip(bars, avg_times):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                f'{time_val:.4f}s', ha='center', va='bottom', fontweight='bold')
    
    plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()

    avg_pg = np.mean(postgres_times)
    avg_mongo = np.mean(mongodb_times)
    
    print(f"\n📋 ДЕТАЛЬНАЯ СТАТИСТИКА:")
    print(f"PostgreSQL - Среднее: {avg_pg:.4f}с")
    print(f"MongoDB - Среднее: {avg_mongo:.4f}с")
    print(f"Соотношение производительности: {avg_mongo/avg_pg:.2f}x")

print("\n✅ АНАЛИЗ ЗАВЕРШЕН!")
```
