#!/usr/bin/env python3
"""
Анализ данных Spotify Tracks с использованием Pandas
Задача: найти музыкальный жанр с максимальной средней популярностью
"""
import pandas as pd
import sys
import os

def load_data(filepath):
    """Загрузить данные из CSV файла"""
    print("Загрузка данных...")
    try:
        # Используем UTF-8 кодировку и оптимизированные параметры
        df = pd.read_csv(
            filepath, 
            encoding='utf-8',
            low_memory=False
        )
        print(f"✅ Успешно загружено строк: {len(df):,}")
        return df
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df):,}")
    
    # Проверим структуру данных
    print(f"Столбцы: {list(df.columns)}")
    
    # Удалить строки без популярности
    initial_count = len(df)
    df = df[df['popularity'].notna()]
    print(f"Удалено строк без популярности: {initial_count - len(df)}")
    
    # Заполнить пустые значения в жанре
    df['genre'] = df['genre'].fillna('Unknown')
    
    print(f"✅ Количество строк после очистки: {len(df):,}")
    print(f"✅ Уникальных жанров: {df['genre'].nunique()}")
    
    return df

def analyze_popularity_by_genre(df):
    """Анализ средней популярности по жанрам"""
    print("\n=== Анализ средней популярности по жанрам ===")
    
    # Группировка по жанру и вычисление статистик
    result = df.groupby('genre')['popularity'].agg([
        ('Mean_Popularity', 'mean'),
        ('Count', 'count'),
        ('Std', 'std'),
        ('Min_Popularity', 'min'),
        ('Max_Popularity', 'max')
    ]).reset_index()
    
    # Сортировка по средней популярности
    result = result.sort_values('Mean_Popularity', ascending=False)
    
    return result

def find_max_mean_genre(df):
    """Найти жанр с максимальной средней популярностью"""
    result = analyze_popularity_by_genre(df)
    
    print("\n=== РЕЗУЛЬТАТЫ ===")
    print("\nТоп-10 жанров по средней популярности:")
    print(result.head(10).round(2).to_string(index=False))
    
    if len(result) > 0:
        max_genre = result.iloc[0]
        print(f"\n🎵 Жанр с максимальной средней популярностью: '{max_genre['genre']}'")
        print(f"📊 Средняя популярность: {max_genre['Mean_Popularity']:.2f}")
        print(f"🎶 Количество треков: {int(max_genre['Count']):,}")
        print(f"📉 Минимальная популярность: {max_genre['Min_Popularity']:.2f}")
        print(f"📈 Максимальная популярность: {max_genre['Max_Popularity']:.2f}")
    else:
        print("❌ Не удалось найти подходящие данные для анализа")
    
    return result

def main():
    # Путь к файлу данных
    data_file = '/opt/data/database.csv'
    
    if not os.path.exists(data_file):
        data_file = 'database.csv'
    
    if not os.path.exists(data_file):
        print(f"❌ Файл не найден: {data_file}")
        sys.exit(1)
    
    file_size = os.path.getsize(data_file) / (1024*1024)
    print(f"=== Анализ данных Spotify Tracks ===")
    print(f"📁 Файл: {data_file}")
    print(f"📊 Размер: {file_size:.1f} MB")
    print(f"🔤 Кодировка: UTF-8")
    
    # Загрузка данных
    df = load_data(data_file)
    
    # Быстрый предпросмотр
    print("\n=== Предпросмотр данных ===")
    print(f"Размер данных: {df.shape[0]:,} строк, {df.shape[1]} столбцов")
    print("\nПервые 3 строки:")
    print(df[['genre', 'artist_name', 'track_name', 'popularity']].head(3))
    
    # Очистка данных
    df_clean = clean_data(df)
    
    # Анализ
    result = find_max_mean_genre(df_clean)
    
    # Сохранить результаты
    output_file = 'results/popularity_by_genre.csv'
    os.makedirs('results', exist_ok=True)
    result.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\n💾 Результаты сохранены в: {output_file}")
    
    return result

if __name__ == '__main__':
    main()
