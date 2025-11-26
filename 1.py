#!/usr/bin/env python3
"""
Анализ Spotify Tracks DB с использованием Pandas
Задача: найти жанр с максимальной средней valence (позитивностью)
Файл данных: ZIP архив с CSV внутри
"""
import pandas as pd
import sys
import os
import zipfile

def load_data_from_zip(zip_filepath, csv_inside_zip):
    try:
        with zipfile.ZipFile(zip_filepath) as z:
            print("Содержимое ZIP архива:", z.namelist())
            with z.open(csv_inside_zip) as f:
                df = pd.read_csv(f,
                                 encoding='latin1',  # замените при необходимости
                                 sep=',',
                                 quotechar='"',
                                 engine='python',
                                 on_bad_lines='skip')
        print(f"Загружено строк: {len(df)}")
        return df
    except Exception as e:
        print(f"Ошибка при загрузке данных из ZIP архива: {e}")
        sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df)}")

    df = df[df['genre'].notna()]
    df = df[df['valence'].notna()]

    print(f"Количество строк после очистки: {len(df)}")
    print(f"Уникальных жанров: {df['genre'].nunique()}")

    return df

def analyze_valence_by_genre(df):
    """Анализ средней valence по жанрам"""
    print("\n=== Анализ средней valence по жанрам ===")
    
    result = df.groupby('genre')['valence'].agg(['mean', 'count', 'std', 'min', 'max']).reset_index()
    result.columns = ['Genre', 'Mean_Valence', 'Count', 'Std', 'Min_Valence', 'Max_Valence']
    result = result.sort_values('Mean_Valence', ascending=False)
    return result

def find_max_mean_genre(df):
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
    zip_file = '/opt/data/database.csv'  # ваш ZIP-файл (с расширением .csv, но по факту ZIP)
    
    if not os.path.exists(zip_file):
        print(f"Файл не найден: {zip_file}")
        sys.exit(1)
    
    # Откройте архив и определите имя CSV внутри
    with zipfile.ZipFile(zip_file) as z:
        namelist = z.namelist()
        print(f"Файлы внутри архива: {namelist}")
        # Предположим, что нужен первый файл с расширением .csv
        csv_name = None
        for name in namelist:
            if name.endswith('.csv'):
                csv_name = name
                break
        if not csv_name:
            print("CSV файл внутри ZIP не найден")
            sys.exit(1)
    
    print("=== Анализ Spotify Tracks DB ===")
    print(f"Чтение файла из архива: {csv_name}")
    
    df = load_data_from_zip(zip_file, csv_name)
    
    print("\n=== Информация о данных ===")
    print(df.info())
    print("\nПервые 5 строк:")
    print(df.head())
    
    df_clean = clean_data(df)
    result = find_max_mean_genre(df_clean)
    
    output_dir = 'results'
    output_file = f'{output_dir}/valence_by_genre.csv'
    os.makedirs(output_dir, exist_ok=True)
    result.to_csv(output_file, index=False)
    print(f"\nРезультаты сохранены в: {output_file}")

if __name__ == '__main__':
    main()
