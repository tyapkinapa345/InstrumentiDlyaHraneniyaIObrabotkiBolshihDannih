#!/usr/bin/env python3
"""
Анализ данных Spotify Tracks с использованием Pandas
Задача: найти музыкальный жанр с максимальной средней популярностью
"""
import pandas as pd
import sys
import os
import csv

def inspect_file(filepath):
    """Проверить структуру файла"""
    print("=== Диагностика файла ===")
    
    # Проверим первые несколько строк вручную
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            print("Первые 5 строк файла:")
            for i, range(5):
                line = f.readline()
                print(f"Строка {i+1}: {line[:200]}...")  # Показываем первые 200 символов
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return False
    return True

def load_data_simple(filepath):
    """Простая загрузка с минимальной обработкой"""
    print("Попытка простой загрузки...")
    try:
        # Самый простой способ
        df = pd.read_csv(filepath, encoding='utf-8')
        print(f"✅ Простая загрузка: {len(df)} строк")
        return df
    except Exception as e:
        print(f"❌ Простая загрузка не удалась: {e}")
        return None

def load_data_robust(filepath):
    """Надежная загрузка с обработкой ошибок"""
    print("Попытка надежной загрузки...")
    try:
        # Пробуем разные разделители
        separators = [',', ';', '\t', '|']
        
        for sep in separators:
            try:
                print(f"Пробуем разделитель: '{sep}'")
                df = pd.read_csv(
                    filepath,
                    encoding='utf-8',
                    sep=sep,
                    engine='python',
                    quoting=csv.QUOTE_MINIMAL,
                    error_bad_lines=False,
                    warn_bad_lines=True
                )
                if len(df) > 0:
                    print(f"✅ Загружено с разделителем '{sep}': {len(df)} строк, {len(df.columns)} столбцов")
                    return df
            except Exception as e:
                print(f"❌ Не удалось с разделителем '{sep}': {e}")
                continue
        return None
    except Exception as e:
        print(f"❌ Надежная загрузка не удалась: {e}")
        return None

def load_data_chunks(filepath):
    """Загрузка по частям"""
    print("Попытка загрузки по частям...")
    try:
        chunks = []
        chunk_size = 10000
        
        for i, chunk in enumerate(pd.read_csv(filepath, encoding='utf-8', chunksize=chunk_size, error_bad_lines=False)):
            chunks.append(chunk)
            print(f"Загружено чанк {i+1}: {len(chunk)} строк")
            
        df = pd.concat(chunks, ignore_index=True)
        print(f"✅ Загрузка по частям: {len(df)} строк")
        return df
    except Exception as e:
        print(f"❌ Загрузка по частям не удалась: {e}")
        return None

def load_data_final(filepath):
    """Финальная попытка загрузки"""
    print("Финальная попытка загрузки...")
    
    # Сначала проверим файл
    if not inspect_file(filepath):
        return None
    
    # Пробуем разные методы
    methods = [
        load_data_simple,
        load_data_robust, 
        load_data_chunks
    ]
    
    for method in methods:
        df = method(filepath)
        if df is not None and len(df) > 0:
            return df
    
    print("❌ Все методы загрузки не удались")
    return None

def clean_data(df):
    """Очистка и подготовка данных"""
    if df is None:
        return None
        
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df):,}")
    print(f"Столбцы: {list(df.columns)}")
    
    # Найдем столбцы с популярностью и жанром
    popularity_col = None
    genre_col = None
    
    # Ищем столбцы по разным возможным названиям
    for col in df.columns:
        col_lower = col.lower()
        if 'popular' in col_lower and popularity_col is None:
            popularity_col = col
        if 'genre' in col_lower and genre_col is None:
            genre_col = col
    
    if popularity_col is None:
        print("❌ Не найден столбец с популярностью")
        print("Доступные столбцы:", df.columns.tolist())
        return None
        
    if genre_col is None:
        print("❌ Не найден столбец с жанром")
        print("Доступные столбцы:", df.columns.tolist())
        return None
    
    print(f"Используем столбец популярности: '{popularity_col}'")
    print(f"Используем столбец жанра: '{genre_col}'")
    
    # Очистка данных
    initial_count = len(df)
    
    # Удаляем строки без популярности
    df = df[df[popularity_col].notna()]
    
    # Преобразуем популярность в числовой тип
    df[popularity_col] = pd.to_numeric(df[popularity_col], errors='coerce')
    df = df[df[popularity_col].notna()]
    
    # Очистка жанра
    df[genre_col] = df[genre_col].fillna('Unknown')
    
    print(f"✅ Количество строк после очистки: {len(df):,}")
    print(f"✅ Уникальных жанров: {df[genre_col].nunique()}")
    
    return df, popularity_col, genre_col

def analyze_popularity_by_genre(df, popularity_col, genre_col):
    """Анализ средней популярности по жанрам"""
    print("\n=== Анализ средней популярности по жанрам ===")
    
    # Базовая статистика
    print(f"Общая статистика популярности:")
    print(f"  Среднее: {df[popularity_col].mean():.2f}")
    print(f"  Медиана: {df[popularity_col].median():.2f}")
    print(f"  Минимум: {df[popularity_col].min():.2f}")
    print(f"  Максимум: {df[popularity_col].max():.2f}")
    
    # Анализ по жанрам
    result = df.groupby(genre_col)[popularity_col].agg([
        ('Mean_Popularity', 'mean'),
        ('Count', 'count'),
        ('Std', 'std'),
        ('Min_Popularity', 'min'),
        ('Max_Popularity', 'max')
    ]).reset_index()
    
    # Сортировка по средней популярности
    result = result.sort_values('Mean_Popularity', ascending=False)
    
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
    
    # Загрузка данных
    df = load_data_final(data_file)
    
    if df is None:
        print("❌ Не удалось загрузить данные")
        sys.exit(1)
    
    # Очистка данных
    cleaned = clean_data(df)
    if cleaned is None:
        print("❌ Не удалось очистить данные")
        sys.exit(1)
    
    df_clean, popularity_col, genre_col = cleaned
    
    # Анализ
    result = analyze_popularity_by_genre(df_clean, popularity_col, genre_col)
    
    # Вывод результатов
    print("\n" + "="*60)
    print("🎵 РЕЗУЛЬТАТЫ АНАЛИЗА")
    print("="*60)
    
    print("\nТоп-10 жанров по средней популярности:")
    display_df = result.head(10).round(2)
    print(display_df.to_string(index=False))
    
    if len(result) > 0:
        max_genre = result.iloc[0]
        print(f"\n🏆 ЛУЧШИЙ ЖАНР:")
        print(f"   Жанр: '{max_genre[genre_col]}'")
        print(f"   Средняя популярность: {max_genre['Mean_Popularity']:.2f}")
        print(f"   Количество треков: {int(max_genre['Count']):,}")
        print(f"   Диапазон: {max_genre['Min_Popularity']:.2f} - {max_genre['Max_Popularity']:.2f}")
    
    # Сохранить результаты
    output_file = 'results/popularity_by_genre.csv'
    os.makedirs('results', exist_ok=True)
    result.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\n💾 Результаты сохранены в: {output_file}")
    
    return result

if __name__ == '__main__':
    main()
