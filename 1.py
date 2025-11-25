#!/usr/bin/env python3
"""
Анализ данных Spotify Tracks DB с использованием Pandas
Задача: найти жанр с максимальной средней популярностью
"""
import pandas as pd
import sys
import os

def load_data(filepath):
    """Загрузить данные из CSV файла с обработкой разных кодировок"""
    encodings = ['utf-8', 'latin-1', 'ISO-8859-1', 'cp1252', 'windows-1252']
    
    for encoding in encodings:
        try:
            print(f"Попытка загрузки с кодировкой: {encoding}")
            df = pd.read_csv(filepath, low_memory=False, encoding=encoding)
            print(f"Загружено строк: {len(df)}")
            print(f"Успешно загружено с кодировкой: {encoding}")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Ошибка при загрузке с {encoding}: {e}")
            continue
    
    # Если все кодировки не сработали, пробуем с обработкой ошибок
    try:
        print("Попытка загрузки с обработкой ошибок кодировки...")
        df = pd.read_csv(filepath, low_memory=False, encoding='utf-8', errors='replace')
        print(f"Загружено строк: {len(df)}")
        return df
    except Exception as e:
        print(f"Все попытки загрузки не удались: {e}")
        sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df)}")
    
    # Проверим доступные колонки
    print("\nДоступные колонки:")
    print(df.columns.tolist())
    
    # Проверим наличие нужных колонок
    if 'popularity' not in df.columns:
        print("Предупреждение: колонка 'popularity' не найдена!")
        # Попробуем найти альтернативные названия
        for col in df.columns:
            if 'popular' in col.lower():
                print(f"Возможная альтернатива: {col}")
    
    if 'genre' not in df.columns:
        print("Предупреждение: колонка 'genre' не найдена!")
        # Попробуем найти альтернативные названия
        for col in df.columns:
            if 'genre' in col.lower() or 'style' in col.lower() or 'type' in col.lower():
                print(f"Возможная альтернатива: {col}")
    
    # Удалить строки без популярности (если колонка существует)
    if 'popularity' in df.columns:
        df = df[df['popularity'].notna()]
    
    # Заполнить пустые значения в жанре (если колонка существует)
    if 'genre' in df.columns:
        df['genre'] = df['genre'].fillna('Unknown')
    else:
        # Если нет колонки genre, создадим фиктивную
        print("Создаем фиктивную колонку 'genre'")
        df['genre'] = 'Unknown'
    
    print(f"Количество строк после очистки: {len(df)}")
    print(f"Уникальных жанров: {df['genre'].nunique()}")
    
    return df

def analyze_popularity_by_genre(df):
    """Анализ средней популярности по жанрам"""
    print("\n=== Анализ средней популярности по жанрам ===")
    
    # Проверим типы данных
    print(f"Тип данных в колонке popularity: {df['popularity'].dtype}")
    
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
    print("\nЖанры по средней популярности (топ-10):")
    print(result.head(10).to_string(index=False))
    
    if len(result) > 0:
        max_genre = result.iloc[0]
        print(f"\nЖанр с максимальной средней популярностью: '{max_genre['Genre']}'")
        print(f"Средняя популярность: {max_genre['Mean_Popularity']:.2f}")
        print(f"Количество треков этого жанра: {int(max_genre['Count'])}")
        print(f"Минимальная популярность: {max_genre['Min_Popularity']:.2f}")
        print(f"Максимальная популярность: {max_genre['Max_Popularity']:.2f}")
    else:
        print("Нет данных для анализа")
    
    return result

def additional_analysis(df):
    """Дополнительный анализ музыкальных характеристик"""
    print("\n=== Дополнительный анализ ===")
    
    # Анализ по энергетике (energy)
    if 'energy' in df.columns:
        energy_analysis = df.groupby('genre')['energy'].mean().sort_values(ascending=False)
        print(f"\nТоп-5 жанров по энергетике:")
        for i, (genre, energy) in enumerate(energy_analysis.head().items()):
            print(f"{i+1}. {genre}: {energy:.3f}")
    
    # Анализ по танцевальности (danceability)
    if 'danceability' in df.columns:
        dance_analysis = df.groupby('genre')['danceability'].mean().sort_values(ascending=False)
        print(f"\nТоп-5 жанров по танцевальности:")
        for i, (genre, dance) in enumerate(dance_analysis.head().items()):
            print(f"{i+1}. {genre}: {dance:.3f}")
    
    # Анализ по акустичности (acousticness)
    if 'acousticness' in df.columns:
        acoustic_analysis = df.groupby('genre')['acousticness'].mean().sort_values(ascending=False)
        print(f"\nТоп-5 жанров по акустичности:")
        for i, (genre, acoustic) in enumerate(acoustic_analysis.head().items()):
            print(f"{i+1}. {genre}: {acoustic:.3f}")

def explore_data(df):
    """Исследование структуры данных"""
    print("\n=== Исследование данных ===")
    print(f"Размер данных: {df.shape}")
    print(f"Колонки: {df.columns.tolist()}")
    print(f"\nТипы данных:")
    print(df.dtypes)
    print(f"\nПервые 3 строки:")
    print(df.head(3))
    print(f"\nБазовая статистика:")
    print(df.describe())

def main():
    # Путь к файлу данных
    data_file = '/opt/data/database.csv'
    
    if not os.path.exists(data_file):
        # Попробовать локальный путь
        data_file = 'database.csv'
    
    if not os.path.exists(data_file):
        print(f"Файл не найден: {data_file}")
        # Попробуем найти файл
        for root, dirs, files in os.walk('/opt'):
            for file in files:
                if 'database' in file and file.endswith('.csv'):
                    data_file = os.path.join(root, file)
                    print(f"Найден файл: {data_file}")
                    break
        else:
            print("Файл database.csv не найден в системе")
            sys.exit(1)
    
    print("=== Анализ данных Spotify Tracks DB ===")
    print(f"Файл: {data_file}")
    
    # Загрузка данных
    df = load_data(data_file)
    
    # Исследование структуры данных
    explore_data(df)
    
    # Очистка данных
    df_clean = clean_data(df)
    
    # Основной анализ популярности по жанрам
    result = find_max_mean_genre(df_clean)
    
    # Дополнительный анализ
    additional_analysis(df_clean)
    
    # Сохранить результаты
    output_dir = 'results'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'popularity_by_genre.csv')
    result.to_csv(output_file, index=False)
    print(f"\nРезультаты сохранены в: {output_file}")
    
    return result

if __name__ == '__main__':
    main()
