#!/usr/bin/env python3
"""
Анализ данных Spotify Tracks DB
Задача: средние показатели "энергичность" и "танцевальность" по жанру
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os
import csv

def explore_file_structure(filepath):
    """Исследовать структуру файла"""
    print(f"\n=== Исследование структуры файла {filepath} ===")
    
    try:
        with open(filepath, 'r', encoding='latin-1') as f:
            lines = []
            for i, line in enumerate(f):
                lines.append(line)
                if i >= 10:
                    break
            
        print("Первые 10 строк файла:")
        for i, line in enumerate(lines):
            print(f"{i+1}: {line[:200]}...")  # Показываем только первые 200 символов
            
    except Exception as e:
        print(f"Ошибка при исследовании файла: {e}")

def load_data(filepath):
    """Загрузить данные из CSV файла"""
    # Исследуем файл
    explore_file_structure(filepath)
    
    try:
        # Пробуем загрузить с разными разделителями
        for sep in [',', ';', '\t', '|']:
            try:
                print(f"\nПопытка загрузки с разделителем '{sep}'...")
                df = pd.read_csv(filepath, sep=sep, encoding='latin-1', low_memory=False, on_bad_lines='skip')
                print(f"Успешно! Строк: {len(df)}, Колонок: {len(df.columns)}")
                return df
            except Exception as e:
                print(f"Не удалось с разделителем '{sep}': {e}")
                continue
    except Exception as e:
        print(f"Ошибка при загрузке: {e}")
        sys.exit(1)
    
    # Если все попытки не удались
    print("Не удалось загрузить файл")
    sys.exit(1)

def clean_data(df):
    """Очистка и подготовка данных"""
    print("\n=== Очистка данных ===")
    print(f"Исходное количество строк: {len(df)}")
    print(f"Исходное количество колонок: {len(df.columns)}")
    print(f"Колонки: {df.columns.tolist()}")
    
    # Находим нужные колонки
    energy_col = None
    dance_col = None
    genre_col = None
    
    for col in df.columns:
        col_lower = str(col).lower()
        if 'energy' in col_lower:
            energy_col = col
        elif 'dance' in col_lower:
            dance_col = col
        elif 'genre' in col_lower or 'style' in col_lower or 'type' in col_lower:
            genre_col = col
    
    print(f"\nНайдены колонки:")
    print(f"  Энергичность: {energy_col}")
    print(f"  Танцевальность: {dance_col}")
    print(f"  Жанр: {genre_col}")
    
    # Если колонки не найдены по названию, ищем по содержимому
    if not energy_col:
        for col in df.columns:
            try:
                sample = pd.to_numeric(df[col].head(100), errors='coerce').dropna()
                if len(sample) > 0 and sample.between(0, 1).all():
                    energy_col = col
                    print(f"  Найдена потенциальная колонка энергичности по значениям: {col}")
                    break
            except:
                continue
    
    if not dance_col:
        for col in df.columns:
            if col == energy_col:
                continue
            try:
                sample = pd.to_numeric(df[col].head(100), errors='coerce').dropna()
                if len(sample) > 0 and sample.between(0, 1).all():
                    dance_col = col
                    print(f"  Найдена потенциальная колонка танцевальности по значениям: {col}")
                    break
            except:
                continue
    
    if not genre_col:
        for col in df.columns:
            if col in [energy_col, dance_col]:
                continue
            try:
                unique_values = df[col].astype(str).unique()
                if len(unique_values) < 100:  # Если уникальных значений не слишком много
                    genre_col = col
                    print(f"  Найдена потенциальная колонка жанра: {col}")
                    break
            except:
                continue
    
    # Переименовываем колонки
    if energy_col:
        df = df.rename(columns={energy_col: 'energy'})
        df['energy'] = pd.to_numeric(df['energy'], errors='coerce')
    else:
        print("Внимание: колонка энергичности не найдена!")
        df['energy'] = np.nan
    
    if dance_col:
        df = df.rename(columns={dance_col: 'danceability'})
        df['danceability'] = pd.to_numeric(df['danceability'], errors='coerce')
    else:
        print("Внимание: колонка танцевальности не найдена!")
        df['danceability'] = np.nan
    
    if genre_col:
        df = df.rename(columns={genre_col: 'genre'})
        df['genre'] = df['genre'].fillna('Unknown').astype(str)
    else:
        print("Внимание: колонка жанра не найдена!")
        df['genre'] = 'Unknown'
    
    # Удаляем строки с NaN в нужных колонках
    initial_count = len(df)
    df = df.dropna(subset=['energy', 'danceability'])
    df = df[df['genre'] != 'Unknown']
    
    print(f"\nПосле очистки:")
    print(f"  Удалено строк: {initial_count - len(df)}")
    print(f"  Осталось строк: {len(df)}")
    print(f"  Уникальных жанров: {df['genre'].nunique()}")
    
    return df

def analyze_genre_stats(df):
    """Анализ средних показателей по жанрам"""
    print("\n=== Анализ средних показателей по жанрам ===")
    
    # Группируем по жанру и вычисляем статистики
    stats = df.groupby('genre').agg({
        'energy': ['mean', 'std', 'count'],
        'danceability': ['mean', 'std', 'count']
    }).round(4)
    
    # Упрощаем названия колонок
    stats.columns = ['energy_mean', 'energy_std', 'energy_count', 
                     'dance_mean', 'dance_std', 'dance_count']
    stats = stats.reset_index()
    
    # Добавляем общее количество треков в жанре
    stats['total_tracks'] = stats[['energy_count', 'dance_count']].max(axis=1)
    
    # Сортируем по среднему energy (по убыванию)
    stats_by_energy = stats.sort_values('energy_mean', ascending=False)
    
    # Сортируем по среднему danceability (по убыванию)
    stats_by_dance = stats.sort_values('dance_mean', ascending=False)
    
    print("\nТоп-10 жанров по энергичности:")
    print(stats_by_energy[['genre', 'energy_mean', 'dance_mean', 'total_tracks']].head(10).to_string(index=False))
    
    print("\nТоп-10 жанров по танцевальности:")
    print(stats_by_dance[['genre', 'dance_mean', 'energy_mean', 'total_tracks']].head(10).to_string(index=False))
    
    # Основная статистика
    print(f"\nОбщая статистика:")
    print(f"  Средняя энергичность по всем трекам: {df['energy'].mean():.4f}")
    print(f"  Средняя танцевальность по всем трекам: {df['danceability'].mean():.4f}")
    print(f"  Всего жанров: {len(stats)}")
    print(f"  Всего треков: {len(df)}")
    
    return stats, stats_by_energy, stats_by_dance

def create_visualizations(stats_by_energy, stats_by_dance):
    """Создание визуализаций"""
    print("\n=== Создание визуализаций ===")
    
    # Настройка стиля графиков
    plt.style.use('seaborn-v0_8-darkgrid')
    sns.set_palette("husl")
    
    # Создаем директорию для графиков
    os.makedirs('results/plots', exist_ok=True)
    
    # Ограничиваем количество жанров для отображения (для читаемости)
    top_n = 15
    
    # 1. Топ жанров по энергичности
    plt.figure(figsize=(14, 8))
    
    top_energy = stats_by_energy.head(top_n)
    bars = plt.barh(range(len(top_energy)), top_energy['energy_mean'], 
                    color='lightcoral', edgecolor='darkred', linewidth=1.5)
    
    plt.yticks(range(len(top_energy)), top_energy['genre'])
    plt.xlabel('Средняя энергичность (0-1)', fontsize=12)
    plt.title(f'Топ-{top_n} жанров по средней энергичности', fontsize=14, fontweight='bold')
    plt.grid(axis='x', alpha=0.3)
    
    # Добавляем значения на столбцы
    for i, (bar, val) in enumerate(zip(bars, top_energy['energy_mean'])):
        plt.text(val + 0.005, bar.get_y() + bar.get_height()/2, 
                f'{val:.3f}', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('results/plots/top_energy_genres.png', dpi=150, bbox_inches='tight')
    
    # 2. Топ жанров по танцевальности
    plt.figure(figsize=(14, 8))
    
    top_dance = stats_by_dance.head(top_n)
    bars = plt.barh(range(len(top_dance)), top_dance['dance_mean'], 
                    color='lightblue', edgecolor='darkblue', linewidth=1.5)
    
    plt.yticks(range(len(top_dance)), top_dance['genre'])
    plt.xlabel('Средняя танцевальность (0-1)', fontsize=12)
    plt.title(f'Топ-{top_n} жанров по средней танцевальности', fontsize=14, fontweight='bold')
    plt.grid(axis='x', alpha=0.3)
    
    # Добавляем значения на столбцы
    for i, (bar, val) in enumerate(zip(bars, top_dance['dance_mean'])):
        plt.text(val + 0.005, bar.get_y() + bar.get_height()/2, 
                f'{val:.3f}', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('results/plots/top_danceability_genres.png', dpi=150, bbox_inches='tight')
    
    # 3. Сравнение топ-10 жанров по обоим показателям
    plt.figure(figsize=(16, 10))
    
    # Берем топ-10 по энергичности
    compare_genres = stats_by_energy.head(10)['genre'].tolist()
    compare_data = stats_by_energy[stats_by_energy['genre'].isin(compare_genres)]
    
    x = np.arange(len(compare_genres))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(14, 8))
    bars1 = ax.bar(x - width/2, compare_data['energy_mean'], width, 
                  label='Энергичность', color='lightcoral', edgecolor='darkred')
    bars2 = ax.bar(x + width/2, compare_data['dance_mean'], width, 
                  label='Танцевальность', color='lightblue', edgecolor='darkblue')
    
    ax.set_xlabel('Жанры', fontsize=12)
    ax.set_ylabel('Среднее значение (0-1)', fontsize=12)
    ax.set_title('Сравнение средних показателей энергичности и танцевальности по жанрам', 
                fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(compare_genres, rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    # Добавляем значения
    def autolabel(bars):
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.3f}',
                       xy=(bar.get_x() + bar.get_width() / 2, height),
                       xytext=(0, 3),
                       textcoords="offset points",
                       ha='center', va='bottom', fontsize=8)
    
    autolabel(bars1)
    autolabel(bars2)
    
    plt.tight_layout()
    plt.savefig('results/plots/energy_vs_danceability.png', dpi=150, bbox_inches='tight')
    
    # 4. Heatmap: корреляция между показателями для топ жанров
    plt.figure(figsize=(10, 8))
    
    # Берем топ-15 жанров по количеству треков
    top_genres_by_count = stats_by_energy.nlargest(15, 'total_tracks')
    
    # Создаем матрицу для heatmap
    heatmap_data = top_genres_by_count[['energy_mean', 'dance_mean']].values
    
    plt.imshow(heatmap_data, aspect='auto', cmap='YlOrRd')
    plt.colorbar(label='Среднее значение')
    plt.xlabel('Показатели')
    plt.ylabel('Жанры')
    plt.title('Heatmap: Энергичность и танцевальность по жанрам', fontsize=14, fontweight='bold')
    plt.xticks([0, 1], ['Энергичность', 'Танцевальность'])
    plt.yticks(range(len(top_genres_by_count)), top_genres_by_count['genre'])
    
    # Добавляем значения в ячейки
    for i in range(len(top_genres_by_count)):
        for j in range(2):
            plt.text(j, i, f'{heatmap_data[i, j]:.3f}', 
                    ha='center', va='center', color='black' if heatmap_data[i, j] < 0.7 else 'white')
    
    plt.tight_layout()
    plt.savefig('results/plots/genre_heatmap.png', dpi=150, bbox_inches='tight')
    
    print("Визуализации сохранены в папке 'results/plots/'")
    print("  1. top_energy_genres.png - Топ жанров по энергичности")
    print("  2. top_danceability_genres.png - Топ жанров по танцевальности")
    print("  3. energy_vs_danceability.png - Сравнение показателей")
    print("  4. genre_heatmap.png - Heatmap значений")

def main():
    """Основная функция"""
    # Путь к файлу данных
    data_file = '/opt/data/database.csv'
    
    if not os.path.exists(data_file):
        data_file = 'database.csv'
    
    if not os.path.exists(data_file):
        print(f"Файл не найден: {data_file}")
        # Попробуем найти файл
        for root, dirs, files in os.walk('/opt'):
            for file in files:
                if 'database' in file.lower() and file.endswith('.csv'):
                    data_file = os.path.join(root, file)
                    print(f"Найден файл: {data_file}")
                    break
    
    if not os.path.exists(data_file):
        print("Файл не найден")
        sys.exit(1)
    
    print("=== Анализ Spotify Tracks DB ===")
    print(f"Файл: {data_file}")
    print("Задача: средние показатели 'энергичность' и 'танцевальность' по жанру")
    
    # Загрузка данных
    df = load_data(data_file)
    
    # Очистка данных
    df_clean = clean_data(df)
    
    # Анализ статистик
    stats, stats_by_energy, stats_by_dance = analyze_genre_stats(df_clean)
    
    # Создание визуализаций
    create_visualizations(stats_by_energy, stats_by_dance)
    
    # Сохранение результатов
    os.makedirs('results', exist_ok=True)
    
    # Сохраняем все статистики
    stats.to_csv('results/genre_statistics.csv', index=False, encoding='utf-8')
    stats_by_energy.to_csv('results/genres_by_energy.csv', index=False, encoding='utf-8')
    stats_by_dance.to_csv('results/genres_by_danceability.csv', index=False, encoding='utf-8')
    
    # Сохраняем сырые данные для топ-20 жанров
    top_genres = stats_by_energy.head(20)['genre'].tolist()
    top_data = df_clean[df_clean['genre'].isin(top_genres)]
    top_data.to_csv('results/top_genres_data.csv', index=False, encoding='utf-8')
    
    print(f"\n=== Результаты сохранены ===")
    print("  results/genre_statistics.csv - полная статистика по жанрам")
    print("  results/genres_by_energy.csv - жанры, отсортированные по энергичности")
    print("  results/genres_by_danceability.csv - жанры, отсортированные по танцевальности")
    print("  results/top_genres_data.csv - исходные данные для топ-20 жанров")
    print("  results/plots/ - папка с визуализациями")
    
    return stats

if __name__ == '__main__':
    main()
