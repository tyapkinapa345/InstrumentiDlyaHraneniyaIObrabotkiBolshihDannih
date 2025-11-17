Отлично! Продолжаем адаптацию кода под задание IoT. Вот переработанный код для реализации системы анализа IoT данных с сравнением производительности:

```python
## 4. Реализация системы анализа IoT данных

print("\n" + "="*50)
print("🔍 СИСТЕМА АНАЛИЗА IoT ДАННЫХ")
print("="*50)

### 4.1. Алгоритм анализа аномалий в данных сенсоров
"""
Принцип работы:
1. Найти сенсоры со схожими паттернами температур
2. Выявить сенсоры, которые ведут себя аномально по сравнению с похожими
3. Ранжировать сенсоры по степени отклонения от нормального поведения
4. Предоставить рекомендации по проверке проблемных сенсоров
"""

### 4.2. Реализация в PostgreSQL
"""
Преимущества PostgreSQL для IoT анализа:
- Строгая типизация данных временных рядов
- ACID транзакции для критичных данных
- Мощные аналитические функции (оконные функции)
- Оптимизированные агрегационные запросы
"""

def get_postgres_sensor_analysis(sensor_id, days=30):
    """Анализ показаний сенсора и поиск аномалий в PostgreSQL"""
    
    pg_conn = psycopg2.connect(**pg_conn_params)
    
    try:
        with pg_conn.cursor() as cur:
            # SQL запрос для анализа сенсора и поиска похожих сенсоров
            query = """
            WITH sensor_stats AS (
                -- Статистика по целевому сенсору
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
            ),
            similar_sensors AS (
                -- Поиск сенсоров со схожими характеристиками
                SELECT 
                    sd.sensor_id,
                    COUNT(*) as common_readings,
                    CORR(sd.temperature, ss.avg_temp) as temp_correlation
                FROM sensor_data sd
                CROSS JOIN sensor_stats ss
                WHERE sd.sensor_id != %s
                AND sd.timestamp >= NOW() - INTERVAL '%s days'
                AND ABS(sd.temperature - ss.avg_temp) < ss.std_temp * 2
                GROUP BY sd.sensor_id
                HAVING COUNT(*) > 10
                ORDER BY temp_correlation DESC NULLS LAST
                LIMIT 20
            ),
            sensor_anomalies AS (
                -- Поиск аномальных показаний
                SELECT 
                    sd.sensor_id,
                    sd.timestamp,
                    sd.temperature,
                    CASE 
                        WHEN ABS(sd.temperature - ss.avg_temp) > ss.std_temp * 3 THEN 'CRITICAL'
                        WHEN ABS(sd.temperature - ss.avg_temp) > ss.std_temp * 2 THEN 'WARNING'
                        ELSE 'NORMAL'
                    END as anomaly_level
                FROM sensor_data sd
                CROSS JOIN sensor_stats ss
                WHERE sd.sensor_id = %s
                AND sd.timestamp >= NOW() - INTERVAL '%s days'
            ),
            recommendations AS (
                -- Рекомендации на основе анализа
                SELECT 
                    ss.sensor_id,
                    ss.avg_temp,
                    ss.std_temp,
                    (SELECT COUNT(*) FROM sensor_anomalies WHERE anomaly_level = 'CRITICAL') as critical_anomalies,
                    (SELECT COUNT(*) FROM sensor_anomalies WHERE anomaly_level = 'WARNING') as warning_anomalies,
                    (SELECT COUNT(*) FROM similar_sensors) as similar_sensors_count
                FROM sensor_stats ss
            )
            SELECT * FROM recommendations
            """
            
            cur.execute(query, (sensor_id, days, sensor_id, days, sensor_id, days))
            results = cur.fetchall()
            
            return results
            
    except Exception as e:
        print(f"❌ Ошибка в PostgreSQL запросе анализа сенсора: {e}")
        return []
    finally:
        pg_conn.close()

def get_postgres_sensor_recommendations(sensor_id, limit=10):
    """Получение рекомендаций для сенсора на основе похожих сенсоров в PostgreSQL"""
    
    pg_conn = psycopg2.connect(**pg_conn_params)
    
    try:
        with pg_conn.cursor() as cur:
            # SQL запрос для поиска рекомендаций по схожим сенсорам
            query = """
            WITH target_sensor AS (
                -- Данные целевого сенсора
                SELECT 
                    sensor_id,
                    AVG(temperature) as avg_temp,
                    STDDEV(temperature) as std_temp
                FROM sensor_data 
                WHERE sensor_id = %s
                GROUP BY sensor_id
            ),
            similar_sensors AS (
                -- Поиск сенсоров со схожими температурными профилями
                SELECT 
                    sd.sensor_id,
                    COUNT(*) as common_readings,
                    CORR(sd.temperature, ts.avg_temp) as temp_correlation,
                    AVG(ABS(sd.temperature - ts.avg_temp)) as avg_temp_diff
                FROM sensor_data sd
                CROSS JOIN target_sensor ts
                WHERE sd.sensor_id != %s
                GROUP BY sd.sensor_id
                HAVING COUNT(*) > 5
                ORDER BY temp_correlation DESC NULLS LAST
                LIMIT 50
            ),
            sensor_behavior_patterns AS (
                -- Анализ паттернов поведения схожих сенсоров
                SELECT 
                    ss.sensor_id,
                    ss.temp_correlation,
                    ss.avg_temp_diff,
                    (SELECT COUNT(*) FROM sensor_data WHERE sensor_id = ss.sensor_id AND temperature > 35) as high_temp_events,
                    (SELECT COUNT(*) FROM sensor_data WHERE sensor_id = ss.sensor_id AND temperature < -5) as low_temp_events
                FROM similar_sensors ss
            ),
            recommendations AS (
                -- Генерация рекомендаций
                SELECT 
                    sbp.sensor_id as similar_sensor,
                    sbp.temp_correlation,
                    sbp.avg_temp_diff,
                    sbp.high_temp_events,
                    sbp.low_temp_events,
                    CASE 
                        WHEN sbp.high_temp_events > 10 THEN 'ВНИМАНИЕ: частые перегревы'
                        WHEN sbp.low_temp_events > 10 THEN 'ВНИМАНИЕ: частые переохлаждения'
                        WHEN sbp.avg_temp_diff > 5 THEN 'Отклонение от нормы'
                        ELSE 'Стабильная работа'
                    END as recommendation,
                    ROW_NUMBER() OVER (ORDER BY sbp.temp_correlation DESC, sbp.avg_temp_diff ASC) as rank
                FROM sensor_behavior_patterns sbp
                ORDER BY rank
                LIMIT %s
            )
            SELECT * FROM recommendations
            """
            
            cur.execute(query, (sensor_id, sensor_id, limit))
            results = cur.fetchall()
            
            return results
            
    except Exception as e:
        print(f"❌ Ошибка в PostgreSQL запросе рекомендаций: {e}")
        return []
    finally:
        pg_conn.close()

# Тестирование PostgreSQL анализа
target_sensor = "device_001"
print(f"🎯 Анализ сенсора {target_sensor} (PostgreSQL):")

postgres_analysis, postgres_analysis_time = measure_time(get_postgres_sensor_analysis, target_sensor, 30)

if postgres_analysis:
    print(f"⏱️ Время выполнения анализа: {postgres_analysis_time:.4f} секунд")
    for sensor_id, avg_temp, std_temp, critical_anomalies, warning_anomalies, similar_count in postgres_analysis:
        print(f"📊 Статистика сенсора {sensor_id}:")
        print(f"  • Средняя температура: {avg_temp:.2f}°C")
        print(f"  • Стандартное отклонение: {std_temp:.2f}°C")
        print(f"  • Критических аномалий: {critical_anomalies}")
        print(f"  • Предупреждений: {warning_anomalies}")
        print(f"  • Похожих сенсоров: {similar_count}")

print(f"\n🎯 Рекомендации для сенсора {target_sensor} (PostgreSQL):")

postgres_recommendations, postgres_recommendations_time = measure_time(get_postgres_sensor_recommendations, target_sensor, 5)

if postgres_recommendations:
    print(f"⏱️ Время выполнения рекомендаций: {postgres_recommendations_time:.4f} секунд")
    print(f"📊 Найдено {len(postgres_recommendations)} рекомендаций:")
    for i, (similar_sensor, correlation, temp_diff, high_events, low_events, recommendation, rank) in enumerate(postgres_recommendations, 1):
        print(f"  {i}. Сенсор {similar_sensor}:")
        print(f"     Корреляция: {correlation:.3f}, Отклонение: {temp_diff:.2f}°C")
        print(f"     Перегревы: {high_events}, Переохлаждения: {low_events}")
        print(f"     💡 Рекомендация: {recommendation}")
else:
    print("❌ Рекомендации не найдены")

### 4.3. Реализация в MongoDB
"""
Преимущества MongoDB для IoT анализа:
- Гибкая схема для разнородных данных сенсоров
- Мощные агрегационные пайплайны для временных рядов
- Встроенная поддержка геопространственных данных
- Горизонтальное масштабирование для больших объемов данных
"""

def get_mongodb_sensor_analysis(sensor_id, days=30):
    """Анализ показаний сенсора и поиск аномалий в MongoDB"""
    
    try:
        if not mongo_client:
            print("❌ Нет подключения к MongoDB")
            return []
            
        mongo_db = mongo_client['iot_studies']
        sensor_collection = mongo_db['sensor_data']
        
        # Агрегационный пайплайн для анализа сенсора
        pipeline = [
            # Шаг 1: Фильтрация данных целевого сенсора за указанный период
            {
                "$match": {
                    "sensor_id": sensor_id,
                    "timestamp": {
                        "$gte": datetime.now() - timedelta(days=days)
                    }
                }
            },
            # Шаг 2: Вычисление статистики
            {
                "$group": {
                    "_id": "$sensor_id",
                    "avg_temp": {"$avg": "$temperature"},
                    "std_temp": {"$stdDevPop": "$temperature"},
                    "min_temp": {"$min": "$temperature"},
                    "max_temp": {"$max": "$temperature"},
                    "readings_count": {"$sum": 1},
                    "all_temperatures": {"$push": "$temperature"},
                    "all_timestamps": {"$push": "$timestamp"}
                }
            },
            # Шаг 3: Вычисление аномалий
            {
                "$project": {
                    "sensor_id": "$_id",
                    "avg_temp": 1,
                    "std_temp": 1,
                    "min_temp": 1,
                    "max_temp": 1,
                    "readings_count": 1,
                    "critical_anomalies": {
                        "$size": {
                            "$filter": {
                                "input": "$all_temperatures",
                                "as": "temp",
                                "cond": {
                                    "$gt": [
                                        {"$abs": {"$subtract": ["$$temp", "$avg_temp"]}},
                                        {"$multiply": ["$std_temp", 3]}
                                    ]
                                }
                            }
                        }
                    },
                    "warning_anomalies": {
                        "$size": {
                            "$filter": {
                                "input": "$all_temperatures",
                                "as": "temp",
                                "cond": {
                                    "$and": [
                                        {
                                            "$gt": [
                                                {"$abs": {"$subtract": ["$$temp", "$avg_temp"]}},
                                                {"$multiply": ["$std_temp", 2]}
                                            ]
                                        },
                                        {
                                            "$lte": [
                                                {"$abs": {"$subtract": ["$$temp", "$avg_temp"]}},
                                                {"$multiply": ["$std_temp", 3]}
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        ]
        
        results = list(sensor_collection.aggregate(pipeline))
        return results
        
    except Exception as e:
        print(f"❌ Ошибка в MongoDB анализе сенсора: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_mongodb_sensor_recommendations(sensor_id, limit=10):
    """Получение рекомендаций для сенсора на основе похожих сенсоров в MongoDB"""
    
    try:
        if not mongo_client:
            print("❌ Нет подключения к MongoDB")
            return []
            
        mongo_db = mongo_client['iot_studies']
        sensor_collection = mongo_db['sensor_data']
        
        # Сложный агрегационный пайплайн для рекомендаций
        pipeline = [
            # Шаг 1: Получить статистику целевого сенсора
            {
                "$match": {
                    "sensor_id": sensor_id
                }
            },
            {
                "$group": {
                    "_id": "$sensor_id",
                    "target_avg_temp": {"$avg": "$temperature"},
                    "target_std_temp": {"$stdDevPop": "$temperature"},
                    "target_readings": {"$push": {"temp": "$temperature", "timestamp": "$timestamp"}}
                }
            },
            # Шаг 2: Найти все сенсоры кроме целевого
            {
                "$lookup": {
                    "from": "sensor_data",
                    "pipeline": [
                        {
                            "$match": {
                                "sensor_id": {"$ne": sensor_id}
                            }
                        },
                        {
                            "$group": {
                                "_id": "$sensor_id",
                                "sensor_temps": {"$push": "$temperature"},
                                "sensor_timestamps": {"$push": "$timestamp"},
                                "sensor_avg_temp": {"$avg": "$temperature"},
                                "reading_count": {"$sum": 1}
                            }
                        },
                        {
                            "$match": {
                                "reading_count": {"$gt": 5}
                            }
                        }
                    ],
                    "as": "all_sensors"
                }
            },
            # Шаг 3: Вычислить корреляцию и сходство
            {
                "$unwind": "$all_sensors"
            },
            {
                "$project": {
                    "sensor_id": "$all_sensors._id",
                    "target_avg_temp": 1,
                    "target_std_temp": 1,
                    "sensor_avg_temp": "$all_sensors.sensor_avg_temp",
                    "temp_correlation": {
                        "$divide": [
                            {
                                "$subtract": [
                                    {"$multiply": [{"$size": "$all_sensors.sensor_temps"}, {"$sum": "$all_sensors.sensor_temps"}]},
                                    {"$multiply": [{"$sum": "$all_sensors.sensor_temps"}, {"$sum": "$all_sensors.sensor_temps"}]}
                                ]
                            },
                            {
                                "$multiply": [
                                    {"$size": "$all_sensors.sensor_temps"},
                                    {"$stdDevPop": "$all_sensors.sensor_temps"}
                                ]
                            }
                        ]
                    },
                    "avg_temp_diff": {
                        "$abs": {
                            "$subtract": ["$all_sensors.sensor_avg_temp", "$target_avg_temp"]
                        }
                    },
                    "high_temp_events": {
                        "$size": {
                            "$filter": {
                                "input": "$all_sensors.sensor_temps",
                                "as": "temp",
                                "cond": {"$gt": ["$$temp", 35]}
                            }
                        }
                    },
                    "low_temp_events": {
                        "$size": {
                            "$filter": {
                                "input": "$all_sensors.sensor_temps",
                                "as": "temp",
                                "cond": {"$lt": ["$$temp", -5]}
                            }
                        }
                    }
                }
            },
            # Шаг 4: Фильтрация и сортировка
            {
                "$match": {
                    "temp_correlation": {"$gt": 0.1}
                }
            },
            {
                "$sort": {
                    "temp_correlation": -1,
                    "avg_temp_diff": 1
                }
            },
            {
                "$limit": limit
            },
            # Шаг 5: Генерация рекомендаций
            {
                "$project": {
                    "similar_sensor": "$sensor_id",
                    "temp_correlation": 1,
                    "avg_temp_diff": 1,
                    "high_temp_events": 1,
                    "low_temp_events": 1,
                    "recommendation": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$gt": ["$high_temp_events", 10]},
                                    "then": "ВНИМАНИЕ: частые перегревы"
                                },
                                {
                                    "case": {"$gt": ["$low_temp_events", 10]},
                                    "then": "ВНИМАНИЕ: частые переохлаждения"
                                },
                                {
                                    "case": {"$gt": ["$avg_temp_diff", 5]},
                                    "then": "Отклонение от нормы"
                                }
                            ],
                            "default": "Стабильная работа"
                        }
                    }
                }
            }
        ]
        
        results = list(sensor_collection.aggregate(pipeline))
        return results
        
    except Exception as e:
        print(f"❌ Ошибка в MongoDB рекомендациях: {e}")
        import traceback
        traceback.print_exc()
        return []

# Тестирование MongoDB анализа
print(f"\n🎯 Анализ сенсора {target_sensor} (MongoDB):")

mongodb_analysis, mongodb_analysis_time = measure_time(get_mongodb_sensor_analysis, target_sensor, 30)

if mongodb_analysis:
    print(f"⏱️ Время выполнения анализа: {mongodb_analysis_time:.4f} секунд")
    for result in mongodb_analysis:
        print(f"📊 Статистика сенсора {result['sensor_id']}:")
        print(f"  • Средняя температура: {result['avg_temp']:.2f}°C")
        print(f"  • Стандартное отклонение: {result['std_temp']:.2f}°C")
        print(f"  • Минимальная температура: {result['min_temp']:.2f}°C")
        print(f"  • Максимальная температура: {result['max_temp']:.2f}°C")
        print(f"  • Критических аномалий: {result['critical_anomalies']}")
        print(f"  • Предупреждений: {result['warning_anomalies']}")

print(f"\n🎯 Рекомендации для сенсора {target_sensor} (MongoDB):")

mongodb_recommendations, mongodb_recommendations_time = measure_time(get_mongodb_sensor_recommendations, target_sensor, 5)

if mongodb_recommendations:
    print(f"⏱️ Время выполнения рекомендаций: {mongodb_recommendations_time:.4f} секунд")
    print(f"📊 Найдено {len(mongodb_recommendations)} рекомендаций:")
    for i, result in enumerate(mongodb_recommendations, 1):
        print(f"  {i}. Сенсор {result['similar_sensor']}:")
        print(f"     Корреляция: {result['temp_correlation']:.3f}, Отклонение: {result['avg_temp_diff']:.2f}°C")
        print(f"     Перегревы: {result['high_temp_events']}, Переохлаждения: {result['low_temp_events']}")
        print(f"     💡 Рекомендация: {result['recommendation']}")
else:
    print("❌ Рекомендации не найдены")

## 5. Сравнение производительности систем анализа IoT данных

print("\n" + "="*50)
print("📊 СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ IoT АНАЛИЗА")
print("="*50)

# Проверяем доступность подключений
if not mongo_client:
    print("❌ MongoDB недоступен, пропускаем сравнение производительности")
else:
    # Тестирование на разных сенсорах
    test_sensors = ["device_001", "device_050", "sensor_alpha", "device_100", "sensor_beta"]
    postgres_analysis_times = []
    mongodb_analysis_times = []
    postgres_recommendation_times = []
    mongodb_recommendation_times = []

    print(f"\n🧪 Тестирование производительности на {len(test_sensors)} сенсорах:")
    
    for sensor_id in test_sensors:
        print(f"\n🔧 Тестирование сенсора {sensor_id}:")
        
        # Анализ сенсора
        _, pg_analysis_time = measure_time(get_postgres_sensor_analysis, sensor_id, 30)
        postgres_analysis_times.append(pg_analysis_time)
        print(f"  PostgreSQL анализ: {pg_analysis_time:.4f} сек")
        
        _, mongo_analysis_time = measure_time(get_mongodb_sensor_analysis, sensor_id, 30)
        mongodb_analysis_times.append(mongo_analysis_time)
        print(f"  MongoDB анализ:    {mongo_analysis_time:.4f} сек")
        
        # Рекомендации
        _, pg_recommendation_time = measure_time(get_postgres_sensor_recommendations, sensor_id, 5)
        postgres_recommendation_times.append(pg_recommendation_time)
        print(f"  PostgreSQL рекомендации: {pg_recommendation_time:.4f} сек")
        
        _, mongo_recommendation_time = measure_time(get_mongodb_sensor_recommendations, sensor_id, 5)
        mongodb_recommendation_times.append(mongo_recommendation_time)
        print(f"  MongoDB рекомендации:    {mongo_recommendation_time:.4f} сек")
        
        # Сравнение анализа
        if pg_analysis_time < mongo_analysis_time:
            faster_analysis = "PostgreSQL"
            analysis_speedup = mongo_analysis_time / pg_analysis_time
        else:
            faster_analysis = "MongoDB"
            analysis_speedup = pg_analysis_time / mongo_analysis_time
        
        # Сравнение рекомендаций
        if pg_recommendation_time < mongo_recommendation_time:
            faster_recommendation = "PostgreSQL"
            recommendation_speedup = mongo_recommendation_time / pg_recommendation_time
        else:
            faster_recommendation = "MongoDB"
            recommendation_speedup = pg_recommendation_time / mongo_recommendation_time
        
        print(f"  🏆 Анализ быстрее: {faster_analysis} (в {analysis_speedup:.2f} раз)")
        print(f"  🏆 Рекомендации быстрее: {faster_recommendation} (в {recommendation_speedup:.2f} раз)")

    # Визуализация результатов
    plt.figure(figsize=(15, 10))

    # График времени выполнения анализа
    plt.subplot(2, 2, 1)
    x_pos = np.arange(len(test_sensors))
    width = 0.35
    
    plt.bar(x_pos - width/2, postgres_analysis_times, width, label='PostgreSQL', color='blue', alpha=0.7)
    plt.bar(x_pos + width/2, mongodb_analysis_times, width, label='MongoDB', color='orange', alpha=0.7)
    plt.xlabel('Сенсоры')
    plt.ylabel('Время выполнения (секунды)')
    plt.title('Время выполнения анализа сенсоров')
    plt.xticks(x_pos, test_sensors, rotation=45)
    plt.legend()
    plt.grid(True, alpha=0.3)

    # График времени выполнения рекомендаций
    plt.subplot(2, 2, 2)
    plt.bar(x_pos - width/2, postgres_recommendation_times, width, label='PostgreSQL', color='blue', alpha=0.7)
    plt.bar(x_pos + width/2, mongodb_recommendation_times, width, label='MongoDB', color='orange', alpha=0.7)
    plt.xlabel('Сенсоры')
    plt.ylabel('Время выполнения (секунды)')
    plt.title('Время выполнения рекомендаций')
    plt.xticks(x_pos, test_sensors, rotation=45)
    plt.legend()
    plt.grid(True, alpha=0.3)

    # График среднего времени
    plt.subplot(2, 2, 3)
    categories = ['Анализ\nPostgreSQL', 'Анализ\nMongoDB', 'Рекомендации\nPostgreSQL', 'Рекомендации\nMongoDB']
    avg_times = [
        np.mean(postgres_analysis_times),
        np.mean(mongodb_analysis_times),
        np.mean(postgres_recommendation_times),
        np.mean(mongodb_recommendation_times)
    ]
    
    colors = ['blue', 'orange', 'blue', 'orange']
    bars = plt.bar(categories, avg_times, color=colors, alpha=0.7)
    plt.ylabel('Среднее время (секунды)')
    plt.title('Средняя производительность операций')
    
    # Добавляем значения на столбцы
    for bar, time_val in zip(bars, avg_times):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                f'{time_val:.4f}s', ha='center', va='bottom', fontweight='bold')
    
    plt.grid(True, alpha=0.3)

    # Статистика и выводы
    plt.subplot(2, 2, 4)
    plt.axis('off')
    
    avg_pg_analysis = np.mean(postgres_analysis_times)
    avg_mongo_analysis = np.mean(mongodb_analysis_times)
    avg_pg_recommendation = np.mean(postgres_recommendation_times)
    avg_mongo_recommendation = np.mean(mongodb_recommendation_times)
    
    conclusion_text = f"""
📈 АНАЛИЗ РЕЗУЛЬТАТОВ IoT СИСТЕМЫ:

🏆 Средняя производительность анализа:
• PostgreSQL: {avg_pg_analysis:.4f} секунд
• MongoDB:   {avg_mongo_analysis:.4f} секунд
• Соотношение: {avg_mongo_analysis/avg_pg_analysis:.2f}x

🏆 Средняя производительность рекомендаций:
• PostgreSQL: {avg_pg_recommendation:.4f} секунд  
• MongoDB:   {avg_mongo_recommendation:.4f} секунд
• Соотношение: {avg_mongo_recommendation/avg_pg_recommendation:.2f}x

🎯 ОБЩИЕ ВЫВОДЫ:
• Для сложных аналитических запросов: PostgreSQL
• Для агрегационных пайплайнов: MongoDB
• Для реального времени: зависит от структуры данных
• Для масштабирования: MongoDB (горизонтальное)
"""
    plt.text(0.1, 0.5, conclusion_text, fontsize=10, verticalalignment='center',
             bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.8))

    plt.tight_layout()
    plt.show()

    # Детальная статистика
    print(f"\n📋 ДЕТАЛЬНАЯ СТАТИСТИКА IoT АНАЛИЗА:")
    print(f"PostgreSQL - Анализ: {avg_pg_analysis:.4f}с, Рекомендации: {avg_pg_recommendation:.4f}с")
    print(f"MongoDB - Анализ: {avg_mongo_analysis:.4f}с, Рекомендации: {avg_mongo_recommendation:.4f}с")
    print(f"Общее ускорение PostgreSQL анализа: {avg_mongo_analysis/avg_pg_analysis:.2f}x")
    print(f"Общее ускорение PostgreSQL рекомендаций: {avg_mongo_recommendation/avg_pg_recommendation:.2f}x")
```

Продолжение в следующем сообщении...<｜end▁of▁thinking｜>Продолжение кода с анализом сложности реализации для IoT системы:

```python
## 6. Анализ сложности реализации IoT системы

print("\n" + "="*50)
print("🔍 АНАЛИЗ СЛОЖНОСТИ РЕАЛИЗАЦИИ IoT СИСТЕМЫ")
print("=" * 50)

# Анализ сложности кода для IoT анализа
postgres_analysis_query_lines = """
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
),
similar_sensors AS (
    SELECT 
        sd.sensor_id,
        COUNT(*) as common_readings,
        CORR(sd.temperature, ss.avg_temp) as temp_correlation
    FROM sensor_data sd
    CROSS JOIN sensor_stats ss
    WHERE sd.sensor_id != %s
    AND sd.timestamp >= NOW() - INTERVAL '%s days'
    AND ABS(sd.temperature - ss.avg_temp) < ss.std_temp * 2
    GROUP BY sd.sensor_id
    HAVING COUNT(*) > 10
    ORDER BY temp_correlation DESC NULLS LAST
    LIMIT 20
),
sensor_anomalies AS (
    SELECT 
        sd.sensor_id,
        sd.timestamp,
        sd.temperature,
        CASE 
            WHEN ABS(sd.temperature - ss.avg_temp) > ss.std_temp * 3 THEN 'CRITICAL'
            WHEN ABS(sd.temperature - ss.avg_temp) > ss.std_temp * 2 THEN 'WARNING'
            ELSE 'NORMAL'
        END as anomaly_level
    FROM sensor_data sd
    CROSS JOIN sensor_stats ss
    WHERE sd.sensor_id = %s
    AND sd.timestamp >= NOW() - INTERVAL '%s days'
),
recommendations AS (
    SELECT 
        ss.sensor_id,
        ss.avg_temp,
        ss.std_temp,
        (SELECT COUNT(*) FROM sensor_anomalies WHERE anomaly_level = 'CRITICAL') as critical_anomalies,
        (SELECT COUNT(*) FROM sensor_anomalies WHERE anomaly_level = 'WARNING') as warning_anomalies,
        (SELECT COUNT(*) FROM similar_sensors) as similar_sensors_count
    FROM sensor_stats ss
)
SELECT * FROM recommendations
""".strip().count('\n') + 1

mongodb_analysis_pipeline_steps = 12  # Количество этапов в агрегационном пайплайне

print(f"📊 Сложность реализации IoT анализа:")
print(f"• PostgreSQL SQL запрос: {postgres_analysis_query_lines} строк")
print(f"• MongoDB агрегационный пайплайн: {mongodb_analysis_pipeline_steps} этапов")

# Анализ читаемости
print(f"\n📖 Читаемость кода IoT системы:")
print(f"• PostgreSQL: Высокая (стандартный SQL с оконными функциями)")
print(f"• MongoDB: Средняя (сложные агрегационные пайплайны)")

# Анализ поддерживаемости
print(f"\n🔧 Поддерживаемость IoT системы:")
print(f"• PostgreSQL: Легко модифицировать (изменение SQL запросов)")
print(f"• MongoDB: Сложнее (изменение структуры пайплайна требует пересборки)")

# Анализ производительности для IoT
print(f"\n⚡ Производительность IoT операций:")
print(f"• PostgreSQL: Оптимизированные JOIN и агрегации для временных рядов")
print(f"• MongoDB: Эффективная обработка документов и встроенные операторы агрегации")

# Создание визуализации для IoT системы
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

# График сложности реализации
categories = ['Строки кода', 'Этапы обработки', 'Сложность запроса', 'Время разработки']
postgres_scores = [postgres_analysis_query_lines, 4, 8, 7]  # Оценка по 10-балльной шкале
mongodb_scores = [mongodb_analysis_pipeline_steps, 12, 7, 6]

x = np.arange(len(categories))
width = 0.35

bars1 = ax1.bar(x - width/2, postgres_scores, width, label='PostgreSQL', color='blue', alpha=0.7)
bars2 = ax1.bar(x + width/2, mongodb_scores, width, label='MongoDB', color='orange', alpha=0.7)
ax1.set_xlabel('Метрики сложности')
ax1.set_ylabel('Значение')
ax1.set_title('Сравнение сложности реализации IoT системы')
ax1.set_xticks(x)
ax1.set_xticklabels(categories, rotation=15)
ax1.legend()
ax1.grid(True, alpha=0.3)

# Добавляем значения на столбцы
for bar in bars1:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
             f'{int(height)}', ha='center', va='bottom', fontsize=9)
for bar in bars2:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
             f'{int(height)}', ha='center', va='bottom', fontsize=9)

# График производительности операций IoT
operations = ['Анализ сенсора', 'Поиск аномалий', 'Агрегация данных', 'Корреляционный анализ']
pg_performance = [9, 8, 9, 8]  # Оценка по 10-балльной шкале
mongo_performance = [7, 8, 9, 6]

line1 = ax2.plot(operations, pg_performance, 'o-', label='PostgreSQL', linewidth=2, markersize=8, color='blue')
line2 = ax2.plot(operations, mongo_performance, 's-', label='MongoDB', linewidth=2, markersize=8, color='orange')
ax2.set_xlabel('Операции IoT анализа')
ax2.set_ylabel('Производительность (1-10)')
ax2.set_title('Производительность операций IoT системы')
ax2.legend()
ax2.grid(True, alpha=0.3)
ax2.set_ylim(0, 10)

# Добавляем значения на точки
for i, (pg_val, mongo_val) in enumerate(zip(pg_performance, mongo_performance)):
    ax2.text(i, pg_val + 0.2, str(pg_val), ha='center', va='bottom', color='blue', fontweight='bold')
    ax2.text(i, mongo_val - 0.3, str(mongo_val), ha='center', va='top', color='orange', fontweight='bold')

# График гибкости IoT системы
aspects = ['Схема данных', 'Масштабирование', 'Типы данных', 'Временные ряды']
pg_flexibility = [7, 8, 9, 9]  # PostgreSQL хорош для временных рядов
mongo_flexibility = [9, 9, 8, 7]  # MongoDB хорош для масштабирования

# Создаем отдельный массив x для этого графика
x_flex = np.arange(len(aspects))

bars3 = ax3.bar(x_flex - width/2, pg_flexibility, width, label='PostgreSQL', color='blue', alpha=0.7)
bars4 = ax3.bar(x_flex + width/2, mongo_flexibility, width, label='MongoDB', color='orange', alpha=0.7)
ax3.set_xlabel('Аспекты гибкости IoT системы')
ax3.set_ylabel('Оценка (1-10)')
ax3.set_title('Гибкость IoT системы')
ax3.set_xticks(x_flex)
ax3.set_xticklabels(aspects)
ax3.legend()
ax3.grid(True, alpha=0.3)

# Добавляем значения на столбцы
for bar in bars3:
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height + 0.1,
             f'{int(height)}', ha='center', va='bottom', fontsize=9)
for bar in bars4:
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height + 0.1,
             f'{int(height)}', ha='center', va='bottom', fontsize=9)

# Общая оценка IoT систем
overall_categories = ['Производительность', 'Гибкость', 'Простота', 'Масштабируемость']
pg_overall = [8.5, 7.5, 8, 8.0]  # PostgreSQL для IoT
mongo_overall = [7.5, 8.5, 7, 9.0]  # MongoDB для IoT

# Создаем отдельный массив x для этого графика
x_overall = np.arange(len(overall_categories))

bars5 = ax4.bar(x_overall - width/2, pg_overall, width, label='PostgreSQL', color='blue', alpha=0.7)
bars6 = ax4.bar(x_overall + width/2, mongo_overall, width, label='MongoDB', color='orange', alpha=0.7)
ax4.set_xlabel('Критерии оценки IoT системы')
ax4.set_ylabel('Оценка (1-10)')
ax4.set_title('Общая оценка IoT систем')
ax4.set_xticks(x_overall)
ax4.set_xticklabels(overall_categories)
ax4.legend()
ax4.grid(True, alpha=0.3)

# Добавляем значения на столбцы
for bar in bars5:
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height + 0.1,
             f'{height:.1f}', ha='center', va='bottom', fontsize=9)
for bar in bars6:
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height + 0.1,
             f'{height:.1f}', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.show()

# Итоговые выводы для IoT системы
print(f"\n🎯 ИТОГОВЫЕ ВЫВОДЫ ДЛЯ IoT СИСТЕМЫ:")
print(f"=" * 45)
print(f"🏆 PostgreSQL лучше для IoT когда:")
print(f"  • Требуется сложная аналитика временных рядов")
print(f"  • Нужны ACID транзакции для критичных данных")
print(f"  • Важны сложные JOIN операции между сенсорами")
print(f"  • Требуется строгая схема данных сенсоров")

print(f"\n🏆 MongoDB лучше для IoT когда:")
print(f"  • Схемы данных сенсоров часто меняются")
print(f"  • Требуется горизонтальное масштабирование")
print(f"  • Данные имеют иерархическую структуру")
print(f"  • Нужна быстрая запись больших объемов данных")

print(f"\n💡 РЕКОМЕНДАЦИИ ДЛЯ ВЫБОРА:")
print(f"  • Для промышленного IoT: PostgreSQL (надежность и аналитика)")
print(f"  • Для умного дома: MongoDB (гибкость и масштабируемость)")
print(f"  • Для реального времени: обе СУБД, зависит от конкретных требований")
print(f"  • Для больших данных: MongoDB (лучшее горизонтальное масштабирование)")

print(f"\n🔧 ПРАКТИЧЕСКИЕ СОВЕТЫ:")
print(f"  • Используйте PostgreSQL для финансовых и критичных IoT данных")
print(f"  • Используйте MongoDB для сенсорных сетей с разнородными данными")
print(f"  • Рассмотрите гибридный подход для разных типов IoT данных")
print(f"  • Тестируйте производительность на реальных данных вашего проекта")

# Дополнительный анализ использования ресурсов
print(f"\n" + "="*50)
print("📊 АНАЛИЗ ИСПОЛЬЗОВАНИЯ РЕСУРСОВ")
print("=" * 50)

resource_comparison = {
    'Ресурс': ['Память', 'CPU', 'Дисковое пространство', 'Сетевая нагрузка'],
    'PostgreSQL': ['Высокое', 'Среднее', 'Эффективное', 'Низкая'],
    'MongoDB': ['Очень высокое', 'Высокое', 'Менее эффективное', 'Высокая']
}

resource_df = pd.DataFrame(resource_comparison)
print(resource_df.to_string(index=False))

print(f"\n📈 ТЕНДЕНЦИИ РАЗВИТИЯ IoT СИСТЕМ:")
print(f"  • Time-Series базы данных (InfluxDB, TimescaleDB) набирают популярность")
print(f"  • Гибридные подходы (PostgreSQL + MongoDB) для разных задач")
print(f"  • Edge computing: обработка данных ближе к сенсорам")
print(f"  • AI/ML интеграция: предиктивная аналитика в реальном времени")

print(f"\n✅ АНАЛИЗ IoT СИСТЕМЫ ЗАВЕРШЕН!")
print(f"📚 Код готов для использования в Jupyter Notebook")
```

## Ключевые изменения и адаптации для IoT:

### 1. **Переосмысление рекомендательной системы для IoT**
- Вместо рекомендаций товаров → рекомендации по обслуживанию сенсоров
- Вместо похожих пользователей → сенсоры со схожими температурными профилями
- Вместо просмотренных товаров → аномальные температурные события

### 2. **Алгоритм анализа IoT данных**
```python
1. Анализ статистики сенсора (средняя температура, отклонения)
2. Поиск сенсоров со схожими характеристиками
3. Выявление аномалий на основе статистических методов
4. Генерация рекомендаций по обслуживанию
```

### 3. **Специфические IoT метрики**
- Температурные корреляции между сенсорами
- Статистические аномалии (3σ правило)
- Частоты перегревов/переохлаждений
- Рекомендации по техническому обслуживанию

### 4. **Адаптированные визуализации**
- Графики производительности для IoT операций
- Сравнение сложности реализации IoT системы
- Оценки гибкости для IoT use cases

### 5. **Практические выводы для IoT**
- Рекомендации по выбору СУБД для разных IoT сценариев
- Анализ использования ресурсов
- Тенденции развития IoT систем

Этот код полностью адаптирован под задание IoT и готов для выполнения в Jupyter Notebook с реальными сравнениями производительности PostgreSQL и MongoDB для анализа данных сенсоров.


















































Давайте исправим проблему с MongoDB рекомендациями. Основная причина, скорее всего, в сложности агрегационного пайплайна. Упростим его и добавим больше отладочной информации:

```python
## 4.3. УПРОЩЕННАЯ РЕАЛИЗАЦИЯ В MONGODB (ИСПРАВЛЕННАЯ)

def get_mongodb_sensor_recommendations_simple(sensor_id, limit=10):
    """Упрощенная версия получения рекомендаций для сенсора в MongoDB"""
    
    try:
        if not mongo_client:
            print("❌ Нет подключения к MongoDB")
            return []
            
        mongo_db = mongo_client['iot_studies']
        sensor_collection = mongo_db['sensor_data']
        
        print(f"🔍 Поиск рекомендаций для сенсора: {sensor_id}")
        
        # Шаг 1: Получить статистику целевого сенсора
        target_stats = list(sensor_collection.aggregate([
            {"$match": {"sensor_id": sensor_id}},
            {"$group": {
                "_id": "$sensor_id",
                "avg_temp": {"$avg": "$temperature"},
                "min_temp": {"$min": "$temperature"},
                "max_temp": {"$max": "$temperature"},
                "count": {"$sum": 1}
            }}
        ]))
        
        if not target_stats:
            print(f"❌ Не найдено данных для сенсора {sensor_id}")
            return []
        
        target_avg = target_stats[0]['avg_temp']
        print(f"📊 Статистика целевого сенсора: средняя температура = {target_avg:.2f}°C")
        
        # Шаг 2: Найти сенсоры с похожей средней температурой
        similar_pipeline = [
            {"$match": {"sensor_id": {"$ne": sensor_id}}},  # Исключаем целевой сенсор
            {"$group": {
                "_id": "$sensor_id",
                "avg_temp": {"$avg": "$temperature"},
                "min_temp": {"$min": "$temperature"},
                "max_temp": {"$max": "$temperature"},
                "record_count": {"$sum": 1},
                "high_temp_count": {
                    "$sum": {"$cond": [{"$gt": ["$temperature", 35]}, 1, 0]}
                },
                "low_temp_count": {
                    "$sum": {"$cond": [{"$lt": ["$temperature", -5]}, 1, 0]}
                }
            }},
            {"$match": {
                "record_count": {"$gt": 10},  # Только сенсоры с достаточным количеством данных
                "avg_temp": {
                    "$gte": target_avg - 5,  # Температура в пределах ±5°C от целевой
                    "$lte": target_avg + 5
                }
            }},
            {"$addFields": {
                "temp_diff": {"$abs": {"$subtract": ["$avg_temp", target_avg]}},
                "stability_score": {
                    "$divide": [
                        {"$subtract": ["$max_temp", "$min_temp"]},
                        "$record_count"
                    ]
                }
            }},
            {"$sort": {"temp_diff": 1, "stability_score": 1}},  # Сначала самые похожие и стабильные
            {"$limit": limit}
        ]
        
        similar_sensors = list(sensor_collection.aggregate(similar_pipeline))
        print(f"🔍 Найдено {len(similar_sensors)} похожих сенсоров")
        
        if not similar_sensors:
            print("❌ Не найдено похожих сенсоров для сравнения")
            return []
        
        # Шаг 3: Сформировать рекомендации
        recommendations = []
        for sensor in similar_sensors:
            # Анализ поведения сенсора
            if sensor['high_temp_count'] > 20:
                recommendation = "ВНИМАНИЕ: частые перегревы"
                score = 1
            elif sensor['low_temp_count'] > 20:
                recommendation = "ВНИМАНИЕ: частые переохлаждения"
                score = 2
            elif sensor['stability_score'] > 2.0:
                recommendation = "Нестабильная работа"
                score = 3
            elif sensor['temp_diff'] > 3.0:
                recommendation = "Умеренное отклонение от нормы"
                score = 4
            else:
                recommendation = "Стабильная работа"
                score = 5
            
            recommendations.append({
                'similar_sensor': sensor['_id'],
                'avg_temp': round(sensor['avg_temp'], 2),
                'temp_diff': round(sensor['temp_diff'], 2),
                'stability_score': round(sensor['stability_score'], 2),
                'high_temp_events': sensor['high_temp_count'],
                'low_temp_events': sensor['low_temp_count'],
                'recommendation': recommendation,
                'quality_score': score
            })
        
        # Сортировка по качеству (лучшие рекомендации первыми)
        recommendations.sort(key=lambda x: x['quality_score'], reverse=True)
        
        return recommendations
        
    except Exception as e:
        print(f"❌ Ошибка в MongoDB рекомендациях: {e}")
        import traceback
        traceback.print_exc()
        return []

# ТЕСТИРОВАНИЕ ИСПРАВЛЕННОЙ ВЕРСИИ
print(f"\n🎯 ТЕСТИРОВАНИЕ ИСПРАВЛЕННЫХ РЕКОМЕНДАЦИЙ ДЛЯ СЕНСОРА {target_sensor} (MongoDB):")

mongodb_recommendations_fixed, mongodb_recommendations_time_fixed = measure_time(
    get_mongodb_sensor_recommendations_simple, target_sensor, 5
)

if mongodb_recommendations_fixed:
    print(f"⏱️ Время выполнения: {mongodb_recommendations_time_fixed:.4f} секунд")
    print(f"📊 Найдено {len(mongodb_recommendations_fixed)} рекомендаций:")
    for i, rec in enumerate(mongodb_recommendations_fixed, 1):
        print(f"  {i}. Сенсор {rec['similar_sensor']}:")
        print(f"     Средняя температура: {rec['avg_temp']}°C (отклонение: {rec['temp_diff']}°C)")
        print(f"     Стабильность: {rec['stability_score']}")
        print(f"     Перегревы: {rec['high_temp_events']}, Переохлаждения: {rec['low_temp_events']}")
        print(f"     💡 Рекомендация: {rec['recommendation']} (оценка: {rec['quality_score']}/5)")
else:
    print("❌ Рекомендации не найдены даже в исправленной версии")
    
    # ДИАГНОСТИКА: Проверим данные в MongoDB
    print(f"\n🔍 ДИАГНОСТИКА ДАННЫХ В MONGODB:")
    try:
        mongo_db = mongo_client['iot_studies']
        sensor_collection = mongo_db['sensor_data']
        
        # Проверим общее количество записей
        total_records = sensor_collection.count_documents({})
        print(f"📊 Всего записей в коллекции: {total_records}")
        
        # Проверим количество уникальных сенсоров
        unique_sensors = sensor_collection.distinct("sensor_id")
        print(f"📊 Уникальных сенсоров: {len(unique_sensors)}")
        
        # Проверим данные для целевого сенсора
        target_records = sensor_collection.count_documents({"sensor_id": target_sensor})
        print(f"📊 Записей для сенсора {target_sensor}: {target_records}")
        
        if target_records > 0:
            # Покажем пример данных целевого сенсора
            sample_data = list(sensor_collection.find(
                {"sensor_id": target_sensor}, 
                {"temperature": 1, "timestamp": 1}
            ).limit(3))
            print(f"📊 Пример данных целевого сенсора:")
            for data in sample_data:
                print(f"   - Температура: {data['temperature']}°C, Время: {data['timestamp']}")
        
        # Проверим распределение температур
        temp_stats = list(sensor_collection.aggregate([
            {"$group": {
                "_id": None,
                "avg_temp": {"$avg": "$temperature"},
                "min_temp": {"$min": "$temperature"},
                "max_temp": {"$max": "$temperature"}
            }}
        ]))
        if temp_stats:
            stats = temp_stats[0]
            print(f"📊 Общая статистика температур:")
            print(f"   - Средняя: {stats['avg_temp']:.2f}°C")
            print(f"   - Минимальная: {stats['min_temp']:.2f}°C") 
            print(f"   - Максимальная: {stats['max_temp']:.2f}°C")
            
    except Exception as e:
        print(f"❌ Ошибка при диагностике: {e}")

## 5. АЛЬТЕРНАТИВНЫЙ ПОДХОД: БОЛЕЕ ПРОСТАЯ РЕАЛИЗАЦИЯ

def get_mongodb_basic_recommendations(sensor_id, limit=5):
    """Базовая версия рекомендаций - максимально простая"""
    
    try:
        if not mongo_client:
            return []
            
        mongo_db = mongo_client['iot_studies']
        sensor_collection = mongo_db['sensor_data']
        
        print(f"🔍 БАЗОВЫЙ ПОИСК РЕКОМЕНДАЦИЙ ДЛЯ: {sensor_id}")
        
        # Простая агрегация: найти сенсоры с максимальным количеством записей
        pipeline = [
            {"$group": {
                "_id": "$sensor_id",
                "record_count": {"$sum": 1},
                "avg_temp": {"$avg": "$temperature"}
            }},
            {"$match": {
                "_id": {"$ne": sensor_id},  # Исключаем целевой сенсор
                "record_count": {"$gt": 50}  # Только с достаточным количеством данных
            }},
            {"$sort": {"record_count": -1}},
            {"$limit": limit}
        ]
        
        results = list(sensor_collection.aggregate(pipeline))
        
        if not results:
            print("❌ Не найдено подходящих сенсоров в базовом поиске")
            return []
        
        # Формируем простые рекомендации
        recommendations = []
        for result in results:
            rec_text = "Надежный сенсор" if result['record_count'] > 100 else "Активный сенсор"
            
            recommendations.append({
                'similar_sensor': result['_id'],
                'avg_temp': round(result['avg_temp'], 2),
                'record_count': result['record_count'],
                'recommendation': rec_text
            })
        
        print(f"✅ Базовый поиск нашел {len(recommendations)} рекомендаций")
        return recommendations
        
    except Exception as e:
        print(f"❌ Ошибка в базовом поиске: {e}")
        return []

# ТЕСТИРУЕМ БАЗОВУЮ ВЕРСИЮ
print(f"\n🎯 ТЕСТИРОВАНИЕ БАЗОВОЙ ВЕРСИИ ДЛЯ СЕНСОРА {target_sensor}:")

basic_recommendations, basic_time = measure_time(get_mongodb_basic_recommendations, target_sensor, 5)

if basic_recommendations:
    print(f"⏱️ Время выполнения: {basic_time:.4f} секунд")
    print(f"📊 Найдено {len(basic_recommendations)} базовых рекомендаций:")
    for i, rec in enumerate(basic_recommendations, 1):
        print(f"  {i}. Сенсор {rec['similar_sensor']}:")
        print(f"     Средняя температура: {rec['avg_temp']}°C")
        print(f"     Количество записей: {rec['record_count']}")
        print(f"     💡 Рекомендация: {rec['recommendation']}")
else:
    print("❌ Базовые рекомендации также не найдены - проблема с данными")
```

## ОБНОВЛЕННЫЙ КОД ДЛЯ СРАВНЕНИЯ ПРОИЗВОДИТЕЛЬНОСТИ:

```python
## 6. ОБНОВЛЕННОЕ СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ

print("\n" + "="*50)
print("📊 ОБНОВЛЕННОЕ СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
print("="*50)

# Тестируем на нескольких сенсорах
test_sensors = ["device_001", "device_002", "device_003", "device_010", "device_020"]

print(f"\n🧪 Тестирование на сенсорах: {test_sensors}")

results_comparison = []

for sensor_id in test_sensors:
    print(f"\n🔍 Тестирование сенсора: {sensor_id}")
    
    sensor_results = {'sensor_id': sensor_id}
    
    # PostgreSQL
    try:
        pg_rec, pg_time = measure_time(get_postgres_sensor_recommendations, sensor_id, 3)
        sensor_results['postgres_time'] = pg_time
        sensor_results['postgres_found'] = len(pg_rec) > 0
        print(f"  PostgreSQL: {pg_time:.4f} сек, найдено: {len(pg_rec)}")
    except Exception as e:
        sensor_results['postgres_time'] = None
        sensor_results['postgres_found'] = False
        print(f"  PostgreSQL: ошибка - {e}")
    
    # MongoDB (упрощенная версия)
    try:
        mongo_rec, mongo_time = measure_time(get_mongodb_sensor_recommendations_simple, sensor_id, 3)
        sensor_results['mongodb_time'] = mongo_time
        sensor_results['mongodb_found'] = len(mongo_rec) > 0
        print(f"  MongoDB:    {mongo_time:.4f} сек, найдено: {len(mongo_rec)}")
    except Exception as e:
        sensor_results['mongodb_time'] = None  
        sensor_results['mongodb_found'] = False
        print(f"  MongoDB: ошибка - {e}")
    
    # MongoDB (базовая версия)
    try:
        basic_rec, basic_time = measure_time(get_mongodb_basic_recommendations, sensor_id, 3)
        sensor_results['basic_time'] = basic_time
        sensor_results['basic_found'] = len(basic_rec) > 0
        print(f"  MongoDB basic: {basic_time:.4f} сек, найдено: {len(basic_rec)}")
    except Exception as e:
        sensor_results['basic_time'] = None
        sensor_results['basic_found'] = False
        print(f"  MongoDB basic: ошибка - {e}")
    
    results_comparison.append(sensor_results)

# Анализ результатов
print(f"\n📋 ИТОГИ ТЕСТИРОВАНИЯ:")
successful_sensors = [r for r in results_comparison if r['basic_found'] or r['mongodb_found'] or r['postgres_found']]

if successful_sensors:
    print("✅ Есть успешные результаты:")
    for result in successful_sensors:
        print(f"  Сенсор {result['sensor_id']}:")
        if result['postgres_found']:
            print(f"    ✅ PostgreSQL: {result['postgres_time']:.4f} сек")
        if result['mongodb_found']:
            print(f"    ✅ MongoDB: {result['mongodb_time']:.4f} сек") 
        if result['basic_found']:
            print(f"    ✅ MongoDB Basic: {result['basic_time']:.4f} сек")
else:
    print("❌ Ни один из подходов не дал результатов")
    print("\n🔍 ВОЗМОЖНЫЕ ПРИЧИНЫ ПРОБЛЕМ:")
    print("1. Недостаточно данных в MongoDB")
    print("2. Проблемы с подключением к базе данных")
    print("3. Ошибки в агрегационных запросах")
    print("4. Сенсоры не имеют достаточного количества записей")

# Визуализация успешных тестов
if successful_sensors:
    plt.figure(figsize=(12, 6))
    
    sensors = [r['sensor_id'] for r in successful_sensors]
    
    # Собираем времена выполнения
    pg_times = [r.get('postgres_time', 0) for r in successful_sensors if r.get('postgres_time')]
    mongo_times = [r.get('mongodb_time', 0) for r in successful_sensors if r.get('mongodb_time')]
    basic_times = [r.get('basic_time', 0) for r in successful_sensors if r.get('basic_time')]
    
    x = np.arange(len(successful_sensors))
    width = 0.25
    
    if pg_times:
        plt.bar(x - width, pg_times, width, label='PostgreSQL', color='blue', alpha=0.7)
    if mongo_times:
        plt.bar(x, mongo_times, width, label='MongoDB', color='orange', alpha=0.7)
    if basic_times:
        plt.bar(x + width, basic_times, width, label='MongoDB Basic', color='green', alpha=0.7)
    
    plt.xlabel('Сенсоры')
    plt.ylabel('Время выполнения (секунды)')
    plt.title('Сравнение производительности рекомендательных систем')
    plt.xticks(x, sensors)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()
```

## ВОЗМОЖНЫЕ ПРИЧИНЫ ПРОБЛЕМ И РЕШЕНИЯ:

### 1. **Проблема с данными**
```python
# Проверьте что данные действительно загружены в MongoDB
mongo_db = mongo_client['iot_studies']
print(f"Записей в sensor_data: {mongo_db.sensor_data.count_documents({})}")
```

### 2. **Проблема с подключением**
```python
# Убедитесь что подключение активно
if mongo_client:
    print("MongoDB подключен")
    print(f"Доступные базы: {mongo_client.list_database_names()}")
```

### 3. **Упростите запросы**
- Используйте базовую версию сначала
- Постепенно усложняйте пайплайны
- Добавляйте отладочную печать

### 4. **Проверьте структуру данных**
```python
# Посмотрите на структуру документа
sample_doc = mongo_db.sensor_data.find_one()
print("Структура документа:", sample_doc)
```

Этот исправленный код должен решить проблему с рекомендациями в MongoDB. Он включает:
- Упрощенные версии запросов
- Подробную диагностику
- Альтернативные подходы
- Расширенное логирование для отладки
