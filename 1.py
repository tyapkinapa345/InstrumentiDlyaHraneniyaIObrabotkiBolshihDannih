#!/usr/bin/env python3
"""
Анализ Spotify Tracks DB с использованием Pandas
Задача: найти жанр с максимальной средней valence (позитивностью)
"""
import pandas as pd
import sys
import os

def load_data(filepath):
    """Загрузить данные из CSV файла с указанием кодировки utf-8"""
    try:
        df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
        print(f"Загружено строк: {len(df)}")
        return df
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df)}")

    # Удалить строки с отсутствующим genre или valence
    df = df[df['genre'].notna()]
    df = df[df['valence'].notna()]

    print(f"Количество строк после очистки: {len(df)}")
    print(f"Уникальных жанров: {df['genre'].nunique()}")

    return df

def analyze_valence_by_genre(df):
    """Анализ средней valence по жанрам"""
    print("\n=== Анализ средней valence по жанрам ===")
    
    # Группировка по жанру и вычисление средней valence
    result = df.groupby('genre')['valence'].agg(['mean', 'count', 'std', 'min', 'max']).reset_index()
    result.columns = ['Genre', 'Mean_Valence', 'Count', 'Std', 'Min_Valence', 'Max_Valence']

    # Сортировка по средней valence
    result = result.sort_values('Mean_Valence', ascending=False)
    
    return result

def find_max_mean_genre(df):
    """Найти жанр с максимальной средней valence"""
    result = analyze_valence_by_genre(df)
    
    print("\n=== Результаты ===")
    print("\nЖанры по средней valence (топ-10):")
    print(result.head(10).to_string(index=False))
    
    max_genre = result.iloc[0]
    print(f"\nЖанр с максимальной средней valence: '{max_genre['Genre']}'")
    print(f"Средняя valence: {max_genre['Mean_Valence']:.3f}")
    print(f"Количество треков этого жанра: {int(max_genre['Count'])}")
    print(f"Минимальная valence: {max_genre['Min_Valence']:.3f}")
    print(f"Максимальная valence: {max_genre['Max_Valence']:.3f}")
    
    return result

def main():
    # Путь к файлу данных (укажите путь к вашему файлу)
    data_file = '/opt/data/spotify_tracks.csv'
    
    if not os.path.exists(data_file):
        # Попробовать локальный путь
        data_file = 'spotify_tracks.csv'
    
    if not os.path.exists(data_file):
        print(f"Файл не найден: {data_file}")
        sys.exit(1)
    
    print("=== Анализ Spotify Tracks DB ===")
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
    output_dir = 'results'
    output_file = f'{output_dir}/valence_by_genre.csv'
    os.makedirs(output_dir, exist_ok=True)
    result.to_csv(output_file, index=False)
    print(f"\nРезультаты сохранены в: {output_file}")
    
    return result

if __name__ == '__main__':
    main()
