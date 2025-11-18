print("="*60)
print("📊 POSTGRESQL: РАБОТА С РЕЛЯЦИОННОЙ БАЗОЙ ДАННЫХ")
print("="*60)

# Параметры подключения к PostgreSQL
pg_conn_params = {
    "dbname": "studpg",
    "user": "postgres",
    "password": "changeme",
    "host": "localhost",  # или "postgresql" для Docker
    "port": "5432"
}

def setup_postgresql():
    """Настройка PostgreSQL и создание таблицы sensor_data"""
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()
        
        # Создание таблицы sensor_data
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
        
        # Создание индексов для оптимизации
        cur.execute("CREATE INDEX idx_sensor_data_sensor_id ON sensor_data(sensor_id)")
        cur.execute("CREATE INDEX idx_sensor_data_timestamp ON sensor_data(timestamp)")
        cur.execute("CREATE INDEX idx_sensor_data_temperature ON sensor_data(temperature)")
        
        print("✅ Таблица sensor_data создана с индексами")
        
        # Загрузка данных
        print("📥 Загрузка данных в PostgreSQL...")
        batch_size = 10000
        for i in range(0, len(iot_df), batch_size):
            batch = iot_df.iloc[i:i+batch_size]
            for _, row in batch.iterrows():
                cur.execute("""
                    INSERT INTO sensor_data (record_id, sensor_id, temperature, timestamp, humidity, pressure, battery_level)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['record_id'], row['sensor_id'], row['temperature'], 
                    row['timestamp'], row['humidity'], row['pressure'], row['battery_level']
                ))
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Загружено {len(iot_df):,} записей в PostgreSQL")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при работе с PostgreSQL: {e}")
        return False

# Настройка PostgreSQL
postgres_ready = setup_postgresql()

def postgres_max_temperature_query():
    """SQL запрос для поиска максимальной температуры по сенсорам"""
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                sensor_id,
                MAX(temperature) as max_temperature,
                COUNT(*) as total_records
            FROM sensor_data
            GROUP BY sensor_id
            ORDER BY max_temperature DESC
        """)
        results = cur.fetchall()
        
        cur.close()
        conn.close()
        return results
        
    except Exception as e:
        print(f"❌ Ошибка в PostgreSQL запросе: {e}")
        return []

if postgres_ready:
    print("\n🔍 ВЫПОЛНЕНИЕ ЗАДАНИЯ: Поиск максимальной температуры для каждого сенсора")
    
    # Измеряем время выполнения
    pg_result, pg_time = measure_time(postgres_max_temperature_query)
    
    print(f"⏱️ Время выполнения PostgreSQL запроса: {pg_time:.4f} секунд")
    print(f"📊 Найдено {len(pg_result)} уникальных сенсоров")
    
    # Показываем топ-5 сенсоров с самой высокой температурой
    print("\n🔥 Топ-5 сенсоров с максимальной температурой (PostgreSQL):")
    for i, (sensor_id, max_temp, count) in enumerate(pg_result[:5]):
        print(f"  {i+1}. {sensor_id}: {max_temp}°C (записей: {count})")
else:
    print("❌ Пропуск выполнения запроса PostgreSQL из-за ошибки настройки")
    pg_time = None
































print("\n" + "="*60)
print("📊 MONGODB: РАБОТА С ДОКУМЕНТО-ОРИЕНТИРОВАННОЙ БАЗОЙ ДАННЫХ")
print("="*60)

def setup_mongodb():
    """Настройка MongoDB и создание коллекции sensor_data"""
    try:
        # Подключение к MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        
        # Проверка подключения
        client.admin.command('ismaster')
        print("✅ Успешное подключение к MongoDB")
        
        db = client['iot_studies']
        
        # Очистка существующей коллекции
        db.sensor_data.drop()
        
        # Загрузка данных в MongoDB
        print("📥 Загрузка данных в MongoDB...")
        collection = db['sensor_data']
        
        # Загрузка данных пачками для оптимизации
        batch_size = 10000
        for i in range(0, len(iot_df), batch_size):
            batch = iot_df.iloc[i:i+batch_size]
            records = batch.to_dict('records')
            collection.insert_many(records)
        
        # Создание индексов для оптимизации
        collection.create_index("sensor_id")
        collection.create_index("timestamp")
        collection.create_index([("sensor_id", 1), ("timestamp", 1)])
        
        print(f"✅ Загружено {len(iot_df):,} записей в MongoDB")
        print("✅ Созданы индексы для оптимизации запросов")
        
        return client
        
    except Exception as e:
        print(f"❌ Ошибка при работе с MongoDB: {e}")
        return None

# Настройка MongoDB
mongo_client = setup_mongodb()

def mongodb_max_temperature_query():
    """Агрегационный запрос MongoDB для поиска максимальной температуры по сенсорам"""
    try:
        db = mongo_client['iot_studies']
        collection = db['sensor_data']
        
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
        return list(collection.aggregate(pipeline))
        
    except Exception as e:
        print(f"❌ Ошибка в MongoDB запросе: {e}")
        return []

if mongo_client:
    print("\n🔍 ВЫПОЛНЕНИЕ ЗАДАНИЯ: Агрегационный запрос для поиска максимальной температуры")
    
    # Измеряем время выполнения
    mongo_result, mongo_time = measure_time(mongodb_max_temperature_query)
    
    print(f"⏱️ Время выполнения MongoDB агрегации: {mongo_time:.4f} секунд")
    print(f"📊 Найдено {len(mongo_result)} уникальных сенсоров")
    
    # Показываем топ-5 сенсоров с самой высокой температурой
    print("\n🔥 Топ-5 сенсоров с максимальной температурой (MongoDB):")
    for i, sensor in enumerate(mongo_result[:5]):
        print(f"  {i+1}. {sensor['_id']}: {sensor['max_temperature']}°C (записей: {sensor['total_records']})")
else:
    print("❌ Пропуск выполнения запроса MongoDB из-за ошибки настройки")
    mongo_time = None





































print("\n" + "="*60)
print("📈 АНАЛИЗ: СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
print("="*60)

if mongo_time is not None and pg_time is not None:
    # Создаем DataFrame для сравнения
    comparison_data = {
        'Database': ['MongoDB', 'PostgreSQL'],
        'Query_Time_Seconds': [mongo_time, pg_time],
        'Records_Processed': [n_records, n_records],
        'Query_Type': ['Aggregation Pipeline', 'SQL GROUP BY'],
        'Speed_Ratio': [mongo_time/pg_time, pg_time/mongo_time]
    }
    
    comparison_df = pd.DataFrame(comparison_data)
    
    print("📊 ТАБЛИЦА СРАВНЕНИЯ ПРОИЗВОДИТЕЛЬНОСТИ:")
    print(comparison_df.to_string(index=False))
    
    # Визуализация сравнения производительности
    plt.figure(figsize=(12, 8))
    
    # График 1: Время выполнения
    plt.subplot(2, 2, 1)
    bars = plt.bar(comparison_df['Database'], comparison_df['Query_Time_Seconds'], 
                   color=['#4CAF50', '#2196F3'], alpha=0.7, edgecolor='black')
    
    plt.title('Время выполнения запросов', fontsize=14, fontweight='bold')
    plt.ylabel('Время (секунды)', fontsize=12)
    plt.xlabel('База данных', fontsize=12)
    
    # Добавляем значения на столбцы
    for bar, time_val in zip(bars, comparison_df['Query_Time_Seconds']):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                f'{time_val:.4f}s', ha='center', va='bottom', fontweight='bold')
    
    # График 2: Соотношение производительности
    plt.subplot(2, 2, 2)
    speed_ratio = mongo_time / pg_time
    colors = ['green' if speed_ratio < 1 else 'red', 'red' if speed_ratio < 1 else 'green']
    plt.bar(['MongoDB/PostgreSQL'], [speed_ratio], color=colors, alpha=0.7)
    plt.axhline(y=1, color='black', linestyle='--', alpha=0.5)
    plt.title('Соотношение производительности\n(MongoDB/PostgreSQL)', fontsize=14, fontweight='bold')
    plt.ylabel('Коэффициент', fontsize=12)
    
    # График 3: Производительность на миллион записей
    plt.subplot(2, 2, 3)
    performance_per_million = [n_records/mongo_time/1000000, n_records/pg_time/1000000]
    bars_perf = plt.bar(comparison_df['Database'], performance_per_million, 
                       color=['#4CAF50', '#2196F3'], alpha=0.7)
    plt.title('Производительность (записей/сек/млн)', fontsize=14, fontweight='bold')
    plt.ylabel('Записей в секунду (млн)', fontsize=12)
    
    for bar, perf in zip(bars_perf, performance_per_million):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                f'{perf:.2f}', ha='center', va='bottom', fontweight='bold')
    
    # График 4: Анализ и выводы
    plt.subplot(2, 2, 4)
    plt.axis('off')
    
    faster_db = 'MongoDB' if mongo_time < pg_time else 'PostgreSQL'
    time_diff = abs(mongo_time - pg_time)
    faster_percent = (time_diff / min(mongo_time, pg_time)) * 100
    
    analysis_text = f"""
📈 РЕЗУЛЬТАТЫ АНАЛИЗА:

⚡ ПРОИЗВОДИТЕЛЬНОСТЬ:
• MongoDB: {mongo_time:.4f} секунд
• PostgreSQL: {pg_time:.4f} секунд
• Разница: {time_diff:.4f} секунд

🏆 ПОБЕДИТЕЛЬ: {faster_db}
• Быстрее на {faster_percent:.1f}%

🔧 ВЫВОДЫ:
• {faster_db} показала лучшую производительность
• Обе СУБД эффективно обработали {n_records:,} записей
• Выбор зависит от конкретных требований проекта
"""
    plt.text(0.1, 0.5, analysis_text, fontsize=11, verticalalignment='center',
             bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.8))
    
    plt.tight_layout()
    plt.show()
    
    # Детальный анализ
    print("\n🔍 ДЕТАЛЬНЫЙ АНАЛИЗ РЕЗУЛЬТАТОВ:")
    print(f"   MongoDB Aggregation Pipeline: {mongo_time:.4f} секунд")
    print(f"   PostgreSQL GROUP BY:          {pg_time:.4f} секунд")
    print(f"   Соотношение (MongoDB/PostgreSQL): {mongo_time/pg_time:.2f}x")
    
    if mongo_time < pg_time:
        print("   • MongoDB показала лучшую производительность для агрегационных операций")
        print("   • Aggregation Pipeline оптимизирован для обработки документов")
    else:
        print("   • PostgreSQL показала лучшую производительность для аналитических запросов") 
        print("   • SQL GROUP BY оптимизирован для реляционных операций")
    
else:
    print("❌ Невозможно выполнить сравнение: отсутствуют данные о времени выполнения")

# Дополнительная статистика
print("\n" + "="*60)
print("📊 ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА ДАННЫХ")
print("="*60)

# Анализ распределения данных по сенсорам
device_stats = iot_df.groupby('sensor_id').agg({
    'temperature': ['count', 'min', 'max', 'mean'],
    'humidity': 'mean',
    'battery_level': 'mean'
}).round(2)

device_stats.columns = ['records', 'min_temp', 'max_temp', 'avg_temp', 'avg_humidity', 'avg_battery']
device_stats = device_stats.sort_values('records', ascending=False)

print("📈 Статистика по сенсорам (топ-5 по количеству записей):")
print(device_stats.head())

print(f"\n📋 ОБЩАЯ СТАТИСТИКА ДАННЫХ:")
print(f"• Всего записей: {len(iot_df):,}")
print(f"• Уникальных сенсоров: {iot_df['sensor_id'].nunique()}")
print(f"• Диапазон температур: {iot_df['temperature'].min():.1f}°C - {iot_df['temperature'].max():.1f}°C")
print(f"• Средняя температура: {iot_df['temperature'].mean():.1f}°C")
print(f"• Период данных: {iot_df['timestamp'].min()} - {iot_df['timestamp'].max()}")
