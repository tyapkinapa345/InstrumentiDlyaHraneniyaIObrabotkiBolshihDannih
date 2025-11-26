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
    try:
        df = pd.read_csv(filepath, low_memory=False)
        print(f"Загружено строк: {len(df)}")
        return df
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df)}")
    
    # Удалить строки без популярности
    df = df[df['popularity'].notna()]
    
    # Заполнить пустые значения в жанре
    df['genre'] = df['genre'].fillna('Unknown')
    
    print(f"Количество строк после очистки: {len(df)}")
    print(f"Уникальных жанров: {df['genre'].nunique()}")
    
    return df

def analyze_popularity_by_genre(df):
    """Анализ средней популярности по жанрам"""
    print("\n=== Анализ средней популярности по жанрам ===")
    
    # Группировка по жанру и вычисление средней популярности
    result = df.groupby('genre')['popularity'].agg(['mean', 'count', 'std', 'min', 'max']).reset_index()
    result.columns = ['Genre', 'Mean_Popularity', 'Count', 'Std', 'Min_Popularity', 'Max_Popularity']
    
    # Сортировка по средней популярности
    result = result.sort_values('Mean_Popularity', ascending=False)
    
    return result

def find_max_mean_genre(df):
    """Найти жанр с максимальной средней популярностью"""
    result = analyze_popularity_by_genre(df)
    
    print("\n=== Результаты ===")
    print("\nЖанры музыки по средней популярности (топ-10):")
    print(result.head(10).to_string(index=False))
    
    max_genre = result.iloc[0]
    print(f"\nЖанр музыки с максимальной средней популярностью: '{max_genre['Genre']}'")
    print(f"Средняя популярность: {max_genre['Mean_Popularity']:.2f}")
    print(f"Количество треков этого жанра: {int(max_genre['Count'])}")
    print(f"Минимальная популярность: {max_genre['Min_Popularity']:.2f}")
    print(f"Максимальная популярность: {max_genre['Max_Popularity']:.2f}")
    
    return result

def main():
    # Путь к файлу данных
    data_file = '/opt/data/database.csv'
    
    if not os.path.exists(data_file):
        # Попробовать локальный путь
        data_file = 'database.csv'
    
    if not os.path.exists(data_file):
        print(f"Файл не найден: {data_file}")
        sys.exit(1)
    
    print("=== Анализ данных Spotify Tracks ===")
    print(f"Файл: {data_file}")
    
    # Загрузка данных
    df = load_data(data_file)
    
    # Показать базовую информацию
    print("\n=== Информация о данных ===")
    print(df.info())
    print("\nПервые 5 строк:")
    print(df.head())
    
    # Очистка данных
    df_clean = clean_data(df)
    
    # Анализ
    result = find_max_mean_genre(df_clean)
    
    # Сохранить результаты
    output_file = 'results/popularity_by_genre.csv'
    os.makedirs('results', exist_ok=True)
    result.to_csv(output_file, index=False)
    print(f"\nРезультаты сохранены в: {output_file}")
    
    return result

if __name__ == '__main__':
    main()
