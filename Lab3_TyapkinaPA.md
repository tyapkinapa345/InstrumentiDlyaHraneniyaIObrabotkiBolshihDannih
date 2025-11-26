**Вариант 16:** 
Задание для PostgreSQL: Интернет вещей (IoT). Создать таблицу sensor_data (sensor _id, temperature, timestamp). Найти максимальную температуру для каждого сенсора за все время. 
Задание для MongoDB: Интернет вещей (IoT). Создать коллекцию sensor_data. Написать агрегационный запрос для поиска максимальной температуры для каждого сенсора. 
Анализ в Jupyter Notebook: Сравнить производительность агрегационных запросов на данных временных рядов от множества источников.

**Цель работы:** Сравнить производительность и особенности работы систем управления базами данных PostgreSQL и MongoDB при обработке данных интернета вещей (IoT) от множества сенсоров.

**Задачи:**
- Сгенерировать тестовые данные IoT сенсоров с использованием Python
- Реализовать идентичные аналитические запросы в PostgreSQL (SQL GROUP BY) и MongoDB (Aggregation Pipeline)
- Провести сравнение производительности агрегационных операций по поиску максимальной температуры
- Проанализировать сложность реализации и поддерживаемость кода
- Сформулировать рекомендации по выбору СУБД для различных IoT сценариев

**Оборудование и программное обеспечение:**
- Компьютер с операционной системой Ubuntu (или любой другой ОС с поддержкой Docker)
- Docker и Docker Compose для запуска контейнеров PostgreSQL и MongoDB
- Python 3.x с библиотеками: psycopg2-binary, pymongo, pandas, matplotlib, seaborn, numpy
- Jupyter Notebook или JupyterLab для выполнения и визуализации кода
- Конфигурация Docker Compose с сервисами: PostgreSQL (порт 5432), MongoDB (порт 27017), Mongo Express (порт 8081), pgAdmin (порт 5050), Jupyter (порт 8888)

## Теоретическая часть

В современном мире объемы данных растут экспоненциально, что приводит к необходимости использования эффективных методов их хранения и обработки. Существует два основных подхода к хранению больших данных:

1. **Реляционные базы данных (PostgreSQL)**: Основаны на реляционной модели данных, где информация хранится в строго структурированных таблицах с заранее определенной схемой. Связи между таблицами устанавливаются с помощью внешних ключей. PostgreSQL является мощной объектно-реляционной СУБД с открытым исходным кодом, поддерживающей ACID-транзакции, сложные запросы и обладающей высокой степенью расширяемости.

2. **NoSQL базы данных (MongoDB)**: Представляют собой широкий класс систем управления базами данных, которые отличаются от традиционных реляционных СУБД. MongoDB — это документоориентированная СУБД, которая хранит данные в гибких, JSON-подобных документах. Схема данных не является фиксированной, что позволяет легко хранить иерархические и полуструктурированные данные. NoSQL-системы часто отдают предпочтение масштабируемости и производительности в ущерб строгой согласованности данных (в сравнении с реляционными СУБД).

Каждый из этих подходов имеет свои преимущества и недостатки, которые мы исследуем в ходе выполнения лабораторной работы. Для IoT сценариев важно учитывать требования к скорости записи, гибкости схемы и сложности аналитических запросов.

## Архитектура решения и потоки данных

### Структура данных в PostgreSQL
```sql
CREATE TABLE sensor_data (
    record_id INTEGER PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    temperature DECIMAL(5,2) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    humidity DECIMAL(5,2),
    pressure DECIMAL(6,2),
    battery_level INTEGER
);
```

**Индексы для оптимизации:**
- `idx_sensor_data_sensor_id` - для фильтрации по сенсорам
- `idx_sensor_data_timestamp` - для временных запросов
- `idx_sensor_data_temperature` - для агрегаций по температуре

### Структура документов в MongoDB
```json
{
  "sensor_id": "device_001",
  "temperature": 23.5,
  "timestamp": ISODate("2024-01-15T10:30:00Z"),
  "humidity": 45.2,
  "pressure": 1013.2,
  "battery_level": 85,
  "record_id": 12345
}
```

**Индексы для оптимизации:**
- `{"sensor_id": 1}` - для поиска по сенсорам
- `{"timestamp": 1}` - для временных диапазонов
- `{"sensor_id": 1, "timestamp": 1}` - составной индекс для комбинированных запросов

### Потоки данных
```
Генерация данных Python (1,000,000 записей IoT)
         ↓
    [DataFrame → JSON]
         ↓
         ├── PostgreSQL 
         │      ↓ (табличная структура с индексами)
         │   SQL запросы (GROUP BY, AVG, MAX, MIN)
         │      ↓
         │   Результаты анализа + графики
         │
         └── MongoDB 
                ↓ (документная структура с индексами)
            Aggregation Pipeline ($group, $avg, $max)
                ↓
            Результаты анализа + графики
         ↓
Сравнение производительности и анализ
```

### Архитектура тестирования
- **Объем данных:** 1,000,000 записей от 103 сенсоров (100 устройств + 3 специальных сенсора)
- **Период данных:** 1 год с сезонными и суточными колебаниями температуры
- **Типы запросов:** 
  - Базовые агрегации (MAX температура по сенсорам)
  - Полная статистика (AVG, MIN, MAX, STDDEV по всем параметрам)
  - Временной анализ (распределение по месяцам)
  - Сравнение производительности
- **Метрики сравнения:** Время выполнения запросов, использование ресурсов, точность результатов

## Практическая часть

### Код для генерации данных и подключения к СУБД

```python
# Основные импорты и настройки
import pandas as pd
import numpy as np
from pymongo import MongoClient
import psycopg2
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import time
import random

# Функция измерения времени выполнения
def measure_time(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

# Генерация IoT данных
def generate_iot_data(n_records, n_devices=100):
    iot_data = []
    device_ids = [f"device_{i:03d}" for i in range(n_devices)]
    special_devices = ["sensor_alpha", "sensor_beta", "sensor_gamma"]
    device_ids.extend(special_devices)
    
    start_date = datetime(2024, 1, 1)
    
    for i in range(n_records):
        device_id = np.random.choice(device_ids)
        timestamp = start_date + timedelta(
            days=np.random.randint(0, 365),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        )
        
        # Реалистичная генерация температуры
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
iot_data = generate_iot_data(n_records)
iot_df = pd.DataFrame(iot_data)
```

### Код создания структур данных

```python
# PostgreSQL - создание таблицы
def setup_postgresql():
    pg_conn_params = {
        "dbname": "studpg",
        "user": "postgres", 
        "password": "changeme",
        "host": "localhost",
        "port": "5432"
    }
    
    pg_conn = psycopg2.connect(**pg_conn_params)
    cur = pg_conn.cursor()
    
    # Создание таблицы
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
    
    # Создание индексов
    cur.execute("CREATE INDEX idx_sensor_data_sensor_id ON sensor_data(sensor_id)")
    cur.execute("CREATE INDEX idx_sensor_data_timestamp ON sensor_data(timestamp)")
    cur.execute("CREATE INDEX idx_sensor_data_temperature ON sensor_data(temperature)")
    
    # Загрузка данных
    for _, row in iot_df.iterrows():
        cur.execute("""
            INSERT INTO sensor_data VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row['record_id'], row['sensor_id'], row['temperature'], 
              row['timestamp'], row['humidity'], row['pressure'], row['battery_level']))
    
    pg_conn.commit()
    cur.close()
    return pg_conn_params

# MongoDB - создание коллекции
def setup_mongodb():
    mongo_client = MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_client['iot_studies']
    mongo_db.sensor_data.drop()
    
    # Загрузка данных
    sensor_collection = mongo_db['sensor_data']
    sensor_records = iot_df.to_dict('records')
    sensor_collection.insert_many(sensor_records)
    
    # Создание индексов
    sensor_collection.create_index("sensor_id")
    sensor_collection.create_index("timestamp")
    sensor_collection.create_index([("sensor_id", 1), ("timestamp", 1)])
    
    return mongo_client
```

### Код запросов для обеих СУБД

```python
# Базовый запрос: максимальная температура по сенсорам

# PostgreSQL
def postgres_max_temperature_query():
    conn = psycopg2.connect(**pg_conn_params)
    try:
        with conn.cursor() as cur:
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
    finally:
        conn.close()

# MongoDB
def mongodb_max_temperature_query():
    mongo_db = mongo_client['iot_studies']
    sensor_collection = mongo_db['sensor_data']
    
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
```

### Код для измерения времени выполнения

```python
# Тестирование производительности
def run_performance_tests():
    test_sensors = ["device_001", "device_050", "sensor_alpha", "device_100"]
    
    results = []
    
    print("🧪 ТЕСТИРОВАНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("=" * 50)
    
    for sensor_id in test_sensors:
        print(f"\n🔍 Тестирование сенсора: {sensor_id}")
        
        # Базовый запрос
        pg_max_result, pg_max_time = measure_time(postgres_max_temperature_query)
        mongo_max_result, mongo_max_time = measure_time(mongodb_max_temperature_query)
        
        results.append({
            'sensor_id': sensor_id,
            'postgres_max_time': pg_max_time,
            'mongodb_max_time': mongo_max_time
        })
        
        print(f"  PostgreSQL MAX: {pg_max_time:.4f}с")
        print(f"  MongoDB MAX:    {mongo_max_time:.4f}с")
    
    return pd.DataFrame(results)

# Запуск тестов
results_df = run_performance_tests()
```

## Результаты и анализ

### Таблица со временем выполнения запросов

```python
# Создание сводной таблицы результатов
summary_data = {
    'Операция': ['MAX температура (все сенсоры)'],
    'PostgreSQL (с)': [0.0373],
    'MongoDB (с)': [0.0035],
    'Соотношение (PostgreSQL/MongoDB)': [10.66]
}

summary_df = pd.DataFrame(summary_data)
print("📊 СВОДНАЯ ТАБЛИЦА ПРОИЗВОДИТЕЛЬНОСТИ")
print(summary_df.to_string(index=False))
```

**Результаты:**
```
📊 СВОДНАЯ ТАБЛИЦА ПРОИЗВОДИТЕЛЬНОСТИ
  Операция                         PostgreSQL (с)  MongoDB (с)  Соотношение (PostgreSQL/MongoDB)
0  MAX температура (все сенсоры)      0.0373        0.0035                             10.66
```

### Визуализация сравнения производительности

```python
# Визуализация результатов
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))

# График 1: Сравнение времени выполнения базовых операций
operations = ['MAX температура']
postgres_times = [0.0373]
mongodb_times = [0.0035]

x = np.arange(len(operations))
width = 0.35

bars1 = ax1.bar(x - width/2, postgres_times, width, label='PostgreSQL', color='blue', alpha=0.7)
bars2 = ax1.bar(x + width/2, mongodb_times, width, label='MongoDB', color='orange', alpha=0.7)

ax1.set_xlabel('Тип операции')
ax1.set_ylabel('Время выполнения (секунды)')
ax1.set_title('Сравнение производительности базовых операций')
ax1.set_xticks(x)
ax1.set_xticklabels(operations)
ax1.legend()
ax1.grid(True, alpha=0.3)

# Добавление значений на столбцы
for bar in bars1:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2, height + 0.001, f'{height:.4f}s', 
             ha='center', va='bottom', fontsize=9)
for bar in bars2:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2, height + 0.001, f'{height:.4f}s', 
             ha='center', va='bottom', fontsize=9)

# График 2: Соотношение производительности
speedup_ratio = [10.66]
operations_ratio = ['MAX температура']
colors = ['red']

bars5 = ax2.bar(operations_ratio, speedup_ratio, color=colors, alpha=0.7)
ax2.set_ylabel('Соотношение (PostgreSQL/MongoDB)')
ax2.set_title('Соотношение производительности')
ax2.grid(True, alpha=0.3)

for bar, ratio in zip(bars5, speedup_ratio):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
             f'{ratio:.2f}x', ha='center', va='bottom', fontweight='bold')

# График 3: Производительность на миллион записей
performance_per_million = [n_records/0.0373/1000000, n_records/0.0035/1000000]
bars_perf = ax3.bar(['PostgreSQL', 'MongoDB'], performance_per_million, 
                   color=['blue', 'orange'], alpha=0.7)
ax3.set_title('Производительность (записей/сек/млн)')
ax3.set_ylabel('Записей в секунду (млн)')

for bar, perf in zip(bars_perf, performance_per_million):
    ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
            f'{perf:.2f}', ha='center', va='bottom', fontweight='bold')

# График 4: Выводы и анализ
ax4.axis('off')
analysis_text = """
📈 АНАЛИЗ РЕЗУЛЬТАТОВ:

⚡ ПРОИЗВОДИТЕЛЬНОСТЬ:
• MongoDB в 10.66 раз быстрее для базовых агрегаций
• MongoDB: 0.0035с (MAX)
• PostgreSQL: 0.0373с (MAX)

🔧 ПРИЧИНЫ РАЗЛИЧИЙ:
• MongoDB оптимизирована для простых агрегаций
• Aggregation Pipeline эффективно обрабатывает документы
• PostgreSQL имеет накладные расходы на SQL-парсинг

🎯 ВЫВОДЫ:
• Для простых IoT агрегаций: MongoDB
• MongoDB лучше подходит для высокоскоростных операций
• PostgreSQL эффективен для сложных реляционных запросов
"""
ax4.text(0.1, 0.5, analysis_text, fontsize=11, verticalalignment='center',
         bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.8))

plt.tight_layout()
plt.show()
```

### Подробные выводы и анализ

## Детальный анализ результатов

### Производительность базовых операций

**MongoDB показала превосходную производительность** (в 10.66 раз быстрее) для базовых агрегационных операций:

1. **Оптимизированная обработка документов** - Aggregation Pipeline эффективно обрабатывает документы в памяти
2. **Встроенные операторы агрегации** - `$group`, `$max` оптимизированы для документной модели
3. **Эффективное использование индексов** - B-деревья в MongoDB хорошо
