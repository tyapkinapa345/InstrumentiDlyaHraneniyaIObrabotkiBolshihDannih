#!/usr/bin/env python3
"""
Анализ музыкальных треков Spotify с использованием Pandas
Задача: найти жанр с максимальной средней популярностью
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
    
    # Группировка по жанру и вычисление статистик
    result = df.groupby('genre')['popularity'].agg(['mean', 'count', 'std', 'min', 'max']).reset_index()
    result.columns = ['Genre', 'Mean_Popularity', 'Count', 'Std', 'Min_Popularity', 'Max_Popularity']
    
    # Сортировка по средней популярности
    result = result.sort_values('Mean_Popularity', ascending=False)
    
    return result

def analyze_audio_features_by_genre(df):
    """Дополнительный анализ аудио-характеристик по жанрам"""
    print("\n=== Анализ аудио-характеристик по жанрам ===")
    
    audio_features = ['danceability', 'energy', 'acousticness', 'instrumentalness', 'valence']
    
    results = {}
    for feature in audio_features:
        feature_stats = df.groupby('genre')[feature].agg(['mean', 'count']).reset_index()
        feature_stats = feature_stats.sort_values('mean', ascending=False)
        results[feature] = feature_stats.head(10)
        
        print(f"\nТоп-10 жанров по {feature}:")
        print(feature_stats.head(10).to_string(index=False))
    
    return results

def find_max_popularity_genre(df):
    """Найти жанр с максимальной средней популярностью"""
    result = analyze_popularity_by_genre(df)
    
    print("\n=== Результаты анализа популярности ===")
    print("\nЖанры по средней популярности (топ-15):")
    print(result.head(15).to_string(index=False))
    
    max_genre = result.iloc[0]
    print(f"\nЖанр с максимальной средней популярностью: '{max_genre['Genre']}'")
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
    
    print("=== Анализ музыкальных треков Spotify ===")
    print(f"Файл: {data_file}")
    
    # Загрузка данных
    df = load_data(data_file)
    
    # Показать базовую информацию
    print("\n=== Информация о данных ===")
    print(f"Всего треков: {len(df)}")
    print(f"Колонки: {list(df.columns)}")
    print(f"Уникальных жанров: {df['genre'].nunique()}")
    
    print("\nПервые 5 строк:")
    print(df.head())
    
    # Очистка данных
    df_clean = clean_data(df)
    
    # Анализ популярности
    popularity_result = find_max_popularity_genre(df_clean)
    
    # Дополнительный анализ аудио-характеристик
    audio_results = analyze_audio_features_by_genre(df_clean)
    
    # Сохранить результаты
    os.makedirs('results', exist_ok=True)
    
    popularity_result.to_csv('results/popularity_by_genre.csv', index=False)
    print(f"\nРезультаты популярности сохранены в: results/popularity_by_genre.csv")
    
    # Сохранить результаты по аудио-характеристикам
    for feature, data in audio_results.items():
        filename = f'results/top_genres_by_{feature}.csv'
        data.to_csv(filename, index=False)
        print(f"Результаты по {feature} сохранены в: {filename}")
    
    return popularity_result, audio_results

if __name__ == '__main__':
    main()
