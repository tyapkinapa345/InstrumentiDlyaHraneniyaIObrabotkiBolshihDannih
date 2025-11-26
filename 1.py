#!/usr/bin/env python3
"""
Анализ данных Spotify Tracks с использованием Pandas
Задача: найти музыкальный жанр с максимальной средней популярностью
"""
import pandas as pd
import sys
import os

def load_data(filepath):
    """Загрузить данные из CSV файла с обработкой ошибок формата"""
    print("Загрузка данных...")
    try:
        # Пробуем разные подходы к загрузке
        try:
            # Первый подход: стандартная загрузка
            df = pd.read_csv(
                filepath, 
                encoding='utf-8',
                low_memory=False,
                on_bad_lines='skip'  # Пропускать проблемные строки
            )
        except TypeError:
            # Для старых версий pandas без on_bad_lines
            df = pd.read_csv(
                filepath, 
                encoding='utf-8',
                low_memory=False,
                error_bad_lines=False,  # Альтернативный параметр
                warn_bad_lines=True
            )
        
        print(f"✅ Успешно загружено строк: {len(df):,}")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        print("Пробуем альтернативный метод загрузки...")
        
        # Альтернативный метод: загрузка с указанием типов данных
        try:
            df = pd.read_csv(
                filepath,
                encoding='utf-8',
                sep=',',
                quotechar='"',
                error_bad_lines=False,
                warn_bad_lines=True
            )
            print(f"✅ Альтернативный метод: загружено строк: {len(df):,}")
            return df
        except Exception as e2:
            print(f"❌ Все методы не удались: {e2}")
            sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df):,}")
    
    # Проверим структуру данных
    print(f"Столбцы: {list(df.columns)}")
    print(f"Типы данных:\n{df.dtypes}")
    
    # Проверим наличие нужных столбцов
    if 'popularity' not in df.columns:
        print("❌ Столбец 'popularity' не найден")
        print("Доступные столбцы:", df.columns.tolist())
        # Попробуем найти альтернативные названия
        for col in df.columns:
            if 'popular' in col.lower():
                print(f"Возможный альтернативный столбец: {col}")
        return None
    
    if 'genre' not in df.columns:
        print("❌ Столбец 'genre' не найден")
        print("Доступные столбцы:", df.columns.tolist())
        return None
    
    # Удалить строки без популярности
    initial_count = len(df)
    df = df[df['popularity'].notna()]
    removed_popularity = initial_count - len(df)
    if removed_popularity > 0:
        print(f"Удалено строк без популярности: {removed_popularity}")
    
    # Преобразовать popularity в числовой тип
    df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
    df = df[df['popularity'].notna()]
    
    # Заполнить пустые значения в жанре
    df['genre'] = df['genre'].fillna('Unknown')
    
    print(f"✅ Количество строк после очистки: {len(df):,}")
    print(f"✅ Уникальных жанров: {df['genre'].nunique()}")
    
    # Быстрая статистика по популярности
    print(f"📊 Статистика популярности:")
    print(f"   Среднее: {df['popularity'].mean():.2f}")
    print(f"   Медиана: {df['popularity'].median():.2f}")
    print(f"   Минимум: {df['popularity'].min():.2f}")
    print(f"   Максимум: {df['popularity'].max():.2f}")
    
    return df

def analyze_popularity_by_genre(df):
    """Анализ средней популярности по жанрам"""
    print("\n=== Анализ средней популярности по жанрам ===")
    
    # Фильтруем жанры с достаточным количеством треков для статистической значимости
    genre_counts = df['genre'].value_counts()
    min_tracks = 10  # минимальное количество треков для анализа
    significant_genres = genre_counts[genre_counts >= min_tracks].index
    df_filtered = df[df['genre'].isin(significant_genres)]
    
    print(f"Анализ для жанров с ≥{min_tracks} треками: {len(df_filtered):,} строк")
    print(f"Количество жанров для анализа: {len(significant_genres)}")
    
    # Группировка по жанру и вычисление статистик
    result = df_filtered.groupby('genre')['popularity'].agg([
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
    if df is None or len(df) == 0:
        print("❌ Нет данных для анализа")
        return None
        
    result = analyze_popularity_by_genre(df)
    
    if len(result) == 0:
        print("❌ Не удалось вычислить статистику по жанрам")
        return None
    
    print("\n" + "="*50)
    print("🎵 РЕЗУЛЬТАТЫ АНАЛИЗА")
    print("="*50)
    
    print("\nТоп-10 жанров по средней популярности:")
    display_df = result.head(10).round(2)
    print(display_df.to_string(index=False))
    
    max_genre = result.iloc[0]
    print(f"\n🏆 ЖАНР-ПОБЕДИТЕЛЬ:")
    print(f"   Название: '{max_genre['genre']}'")
    print(f"   Средняя популярность: {max_genre['Mean_Popularity']:.2f}")
    print(f"   Количество треков: {int(max_genre['Count']):,}")
    print(f"   Диапазон популярности: {max_genre['Min_Popularity']:.2f} - {max_genre['Max_Popularity']:.2f}")
    
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
    print(df.head(3))
    
    # Очистка данных
    df_clean = clean_data(df)
    
    if df_clean is None or len(df_clean) == 0:
        print("❌ Не удалось подготовить данные для анализа")
        sys.exit(1)
    
    # Анализ
    result = find_max_mean_genre(df_clean)
    
    # Сохранить результаты
    if result is not None:
        output_file = 'results/popularity_by_genre.csv'
        os.makedirs('results', exist_ok=True)
        result.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\n💾 Результаты сохранены в: {output_file}")
    
    return result

if __name__ == '__main__':
    main()
