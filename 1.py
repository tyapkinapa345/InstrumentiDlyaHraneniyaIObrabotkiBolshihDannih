#!/usr/bin/env python3
"""
Анализ данных Spotify Tracks с использованием Pandas
Задача: найти музыкальный жанр с максимальной средней популярностью
"""
import pandas as pd
import sys
import os

def detect_encoding(filepath):
    """Определить кодировку файла"""
    import chardet
    with open(filepath, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def load_data(filepath):
    """Загрузить данные из CSV файла"""
    try:
        # Сначала пробуем стандартную загрузку
        df = pd.read_csv(filepath, low_memory=False)
        print(f"Загружено строк: {len(df)}")
        return df
    except UnicodeDecodeError:
        # Если возникает ошибка кодировки, пробуем разные варианты
        print("Обнаружена проблема с кодировкой, пробуем альтернативные варианты...")
        encodings = ['latin-1', 'ISO-8859-1', 'cp1252', 'windows-1252']
        
        for encoding in encodings:
            try:
                print(f"Попытка загрузки с кодировкой: {encoding}")
                df = pd.read_csv(filepath, encoding=encoding, low_memory=False)
                print(f"Успешно загружено с кодировкой: {encoding}")
                print(f"Загружено строк: {len(df)}")
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                continue
        
        # Если все варианты не сработали, пробуем определить кодировку автоматически
        try:
            detected_encoding = detect_encoding(filepath)
            print(f"Автоопределенная кодировка: {detected_encoding}")
            df = pd.read_csv(filepath, encoding=detected_encoding, low_memory=False)
            print(f"Загружено строк: {len(df)}")
            return df
        except:
            print("Не удалось определить кодировку файла")
            sys.exit(1)
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df)}")
    
    # Проверим названия столбцов
    print("Столбцы в данных:", df.columns.tolist())
    
    # Удалить строки без популярности
    if 'popularity' in df.columns:
        df = df[df['popularity'].notna()]
    else:
        print("Столбец 'popularity' не найден в данных")
        print("Доступные столбцы:", df.columns.tolist())
        sys.exit(1)
    
    # Заполнить пустые значения в жанре
    if 'genre' in df.columns:
        df['genre'] = df['genre'].fillna('Unknown')
    else:
        print("Столбец 'genre' не найден в данных")
        print("Доступные столбцы:", df.columns.tolist())
        sys.exit(1)
    
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
    
    # Проверим размер файла
    file_size = os.path.getsize(data_file)
    print(f"Размер файла: {file_size / (1024*1024):.2f} MB")
    
    # Загрузка данных
    df = load_data(data_file)
    
    # Показать базовую информацию
    print("\n=== Информация о данных ===")
    print(f"Размер данных: {df.shape}")
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
