#!/usr/bin/env python3
"""
Анализ данных Spotify Tracks с использованием MapReduce подхода в Pandas
Задача: вычислить средние показатели "энергичность" и "танцевальность" по жанрам
"""
import pandas as pd
import sys
import os
from functools import reduce

def load_data(filepath):
    """Загрузить данные из CSV файла (Mapper-подход)"""
    print("=== MAP: Загрузка данных ===")
    try:
        # Загружаем данные с обработкой ошибок
        df = pd.read_csv(
            filepath,
            encoding='utf-8',
            on_bad_lines='skip',
            low_memory=False
        )
        print(f"✅ Загружено строк: {len(df):,}")
        return df
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        sys.exit(1)

def clean_and_filter_data(df):
    """Очистка и фильтрация данных (Mapper-подход)"""
    print("\n=== MAP: Очистка и фильтрация данных ===")
    print(f"Исходное количество строк: {len(df):,}")
    
    # Проверяем наличие необходимых столбцов
    required_columns = ['genre', 'energy', 'danceability']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"❌ Отсутствуют необходимые столбцы: {missing_columns}")
        print("Доступные столбцы:", df.columns.tolist())
        return None
    
    # Удаляем строки с отсутствующими значениями в нужных столбцах
    initial_count = len(df)
    df = df[required_columns].dropna()
    removed_count = initial_count - len(df)
    
    if removed_count > 0:
        print(f"Удалено строк с пропущенными значениями: {removed_count}")
    
    # Преобразуем числовые столбцы
    df['energy'] = pd.to_numeric(df['energy'], errors='coerce')
    df['danceability'] = pd.to_numeric(df['danceability'], errors='coerce')
    
    # Удаляем некорректные значения (вне диапазона 0-1)
    df = df[(df['energy'] >= 0) & (df['energy'] <= 1)]
    df = df[(df['danceability'] >= 0) & (df['danceability'] <= 1)]
    
    print(f"✅ Количество строк после очистки: {len(df):,}")
    print(f"✅ Уникальных жанров: {df['genre'].nunique()}")
    
    return df

def map_to_key_value_pairs(df):
    """Преобразование данных в пары ключ-значение (Mapper)"""
    print("\n=== MAP: Преобразование в пары ключ-значение ===")
    
    # Создаем список пар (жанр, (энергичность, танцевальность, счетчик))
    mapped_data = []
    
    for _, row in df.iterrows():
        key = row['genre']
        value = {
            'energy_sum': row['energy'],
            'danceability_sum': row['danceability'],
            'count': 1
        }
        mapped_data.append((key, value))
    
    print(f"Создано {len(mapped_data)} пар ключ-значение")
    return mapped_data

def shuffle_and_sort(mapped_data):
    """Группировка данных по ключу (Shuffle & Sort)"""
    print("\n=== SHUFFLE & SORT: Группировка данных по жанрам ===")
    
    # Создаем словарь для группировки
    shuffled_data = {}
    
    for key, value in mapped_data:
        if key not in shuffled_data:
            shuffled_data[key] = []
        shuffled_data[key].append(value)
    
    print(f"Группировано по {len(shuffled_data)} уникальным жанрам")
    return shuffled_data

def reduce_per_genre(shuffled_data):
    """Агрегация данных по жанрам (Reducer)"""
    print("\n=== REDUCE: Агрегация данных по жанрам ===")
    
    reduced_results = []
    
    for genre, values_list in shuffled_data.items():
        # Инициализируем агрегаторы
        total_energy = 0.0
        total_danceability = 0.0
        total_count = 0
        
        # Суммируем все значения для данного жанра
        for value in values_list:
            total_energy += value['energy_sum']
            total_danceability += value['danceability_sum']
            total_count += value['count']
        
        # Вычисляем средние значения
        avg_energy = total_energy / total_count if total_count > 0 else 0
        avg_danceability = total_danceability / total_count if total_count > 0 else 0
        
        reduced_results.append({
            'genre': genre,
            'avg_energy': avg_energy,
            'avg_danceability': avg_danceability,
            'track_count': total_count,
            'total_energy': total_energy,
            'total_danceability': total_danceability
        })
    
    print(f"Агрегировано данных для {len(reduced_results)} жанров")
    return reduced_results

def pandas_mapreduce_approach(df):
    """Альтернативный подход: использование встроенных функций Pandas (оптимизированный)"""
    print("\n=== PANDAS MAPREDUCE: Оптимизированный расчет ===")
    
    # MAP: Группировка по жанру
    grouped = df.groupby('genre')
    
    # REDUCE: Вычисление агрегированных значений
    result = grouped.agg({
        'energy': ['mean', 'sum', 'count'],
        'danceability': ['mean', 'sum', 'count']
    }).reset_index()
    
    # Упрощаем структуру DataFrame
    result.columns = [
        'genre',
        'avg_energy', 'total_energy', 'energy_count',
        'avg_danceability', 'total_danceability', 'danceability_count'
    ]
    
    # Проверяем согласованность счетчиков
    result['track_count'] = result[['energy_count', 'danceability_count']].min(axis=1)
    result = result.drop(['energy_count', 'danceability_count'], axis=1)
    
    # Сортировка по среднему значению энергичности
    result = result.sort_values('avg_energy', ascending=False)
    
    return result

def analyze_results(reduced_data, approach_name="MapReduce"):
    """Анализ и вывод результатов"""
    print(f"\n=== РЕЗУЛЬТАТЫ ({approach_name}) ===")
    
    # Преобразуем в DataFrame для удобства
    if isinstance(reduced_data, list):
        result_df = pd.DataFrame(reduced_data)
    else:
        result_df = reduced_data
    
    # Сортировка по среднему значению энергичности
    result_df = result_df.sort_values('avg_energy', ascending=False)
    
    print(f"\nТоп-10 жанров по средней энергичности:")
    print(result_df[['genre', 'avg_energy', 'avg_danceability', 'track_count']]
          .head(10).round(3).to_string(index=False))
    
    print(f"\nТоп-10 жанров по средней танцевальности:")
    danceability_sorted = result_df.sort_values('avg_danceability', ascending=False)
    print(danceability_sorted[['genre', 'avg_danceability', 'avg_energy', 'track_count']]
          .head(10).round(3).to_string(index=False))
    
    # Находим жанры с максимальными значениями
    max_energy = result_df.loc[result_df['avg_energy'].idxmax()]
    max_danceability = result_df.loc[result_df['avg_danceability'].idxmax()]
    
    print(f"\n🎵 ЖАНР С МАКСИМАЛЬНОЙ ЭНЕРГИЧНОСТЬЮ:")
    print(f"   Жанр: '{max_energy['genre']}'")
    print(f"   Средняя энергичность: {max_energy['avg_energy']:.3f}")
    print(f"   Средняя танцевальность: {max_energy['avg_danceability']:.3f}")
    print(f"   Количество треков: {int(max_energy['track_count']):,}")
    
    print(f"\n💃 ЖАНР С МАКСИМАЛЬНОЙ ТАНЦЕВАЛЬНОСТЬЮ:")
    print(f"   Жанр: '{max_danceability['genre']}'")
    print(f"   Средняя танцевальность: {max_danceability['avg_danceability']:.3f}")
    print(f"   Средняя энергичность: {max_danceability['avg_energy']:.3f}")
    print(f"   Количество треков: {int(max_danceability['track_count']):,}")
    
    # Корреляция между энергичностью и танцевальностью
    correlation = result_df['avg_energy'].corr(result_df['avg_danceability'])
    print(f"\n📊 КОРРЕЛЯЦИЯ МЕЖДУ ПОКАЗАТЕЛЯМИ:")
    print(f"   Корреляция энергичность-танцевальность: {correlation:.3f}")
    
    return result_df

def main():
    # Путь к файлу данных
    data_file = '/opt/data/database.csv'
    
    if not os.path.exists(data_file):
        data_file = 'database.csv'
    
    if not os.path.exists(data_file):
        print(f"❌ Файл не найден: {data_file}")
        sys.exit(1)
    
    file_size = os.path.getsize(data_file) / (1024*1024)
    print("=" * 60)
    print("Анализ Spotify Tracks: Энергичность и Танцевальность по жанрам")
    print("=" * 60)
    print(f"📁 Файл: {data_file}")
    print(f"📊 Размер: {file_size:.1f} MB")
    
    # 1. Загрузка данных
    df = load_data(data_file)
    
    # 2. Очистка данных
    df_clean = clean_and_filter_data(df)
    if df_clean is None:
        print("❌ Не удалось подготовить данные для анализа")
        sys.exit(1)
    
    # 3. Подход 1: Классический MapReduce
    print("\n" + "=" * 60)
    print("ПОДХОД 1: КЛАССИЧЕСКИЙ MAPREDUCE")
    print("=" * 60)
    
    # MAP
    mapped_data = map_to_key_value_pairs(df_clean)
    
    # SHUFFLE & SORT
    shuffled_data = shuffle_and_sort(mapped_data)
    
    # REDUCE
    reduced_data = reduce_per_genre(shuffled_data)
    
    # Анализ результатов
    result_classic = analyze_results(reduced_data, "Классический MapReduce")
    
    # 4. Подход 2: Оптимизированный Pandas MapReduce
    print("\n" + "=" * 60)
    print("ПОДХОД 2: ОПТИМИЗИРОВАННЫЙ PANDAS MAPREDUCE")
    print("=" * 60)
    
    result_pandas = pandas_mapreduce_approach(df_clean)
    result_pandas = analyze_results(result_pandas, "Pandas MapReduce")
    
    # 5. Сохранение результатов
    output_dir = 'results'
    os.makedirs(output_dir, exist_ok=True)
    
    # Сохраняем оба набора результатов
    classic_output = os.path.join(output_dir, 'energy_danceability_classic_mapreduce.csv')
    pandas_output = os.path.join(output_dir, 'energy_danceability_pandas_mapreduce.csv')
    
    pd.DataFrame(result_classic).to_csv(classic_output, index=False, encoding='utf-8')
    result_pandas.to_csv(pandas_output, index=False, encoding='utf-8')
    
    print(f"\n💾 Результаты сохранены:")
    print(f"   {classic_output}")
    print(f"   {pandas_output}")
    
    # 6. Сравнение подходов
    print("\n" + "=" * 60)
    print("СРАВНЕНИЕ ПОДХОДОВ")
    print("=" * 60)
    
    # Проверяем согласованность результатов
    if isinstance(result_classic, list):
        classic_df = pd.DataFrame(result_classic)
    else:
        classic_df = result_classic
    
    # Сравниваем средние значения
    classic_sorted = classic_df.sort_values('genre').reset_index(drop=True)
    pandas_sorted = result_pandas.sort_values('genre').reset_index(drop=True)
    
    # Проверяем совпадение для первых 5 жанров
    print("Сравнение средних значений для топ-5 жанров:")
    for i in range(min(5, len(classic_sorted), len(pandas_sorted))):
        genre = classic_sorted.loc[i, 'genre']
        classic_energy = classic_sorted.loc[i, 'avg_energy']
        pandas_energy = pandas_sorted.loc[i, 'avg_energy']
        
        diff = abs(classic_energy - pandas_energy)
        match = "✅" if diff < 0.001 else "⚠️"
        
        print(f"{match} {genre}:")
        print(f"  Классический: {classic_energy:.4f}, Pandas: {pandas_energy:.4f}")
    
    return result_pandas

if __name__ == '__main__':
    main()
