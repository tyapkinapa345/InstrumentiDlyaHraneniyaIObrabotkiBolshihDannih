#!/usr/bin/env python3
"""
Анализ Spotify треков для поиска жанра с максимальной средней valence (позитивностью).
"""

import os
import sys
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
from hdfs import InsecureClient

# --- 1. Загрузка данных из HDFS или локальных путей ---

def download_file_from_hdfs(hdfs_path: str, local_path: str, java_home: str = '/usr/lib/jvm/java-11-openjdk-amd64') -> bool:
    """
    Скачивает файл из HDFS по заданному пути локально.
    Возвращает True если скачивание успешно, иначе False.
    """
    print(f"Попытка скачать файл из HDFS: {hdfs_path} -> {local_path}")
    cmd = f"hdfs dfs -get {hdfs_path} {local_path}"
    env = dict(os.environ, **{'JAVA_HOME': java_home})
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
    if result.returncode == 0:
        print("Файл успешно загружен из HDFS.")
        return True
    else:
        print(f"Ошибка загрузки из HDFS: {result.stderr.strip()}")
        return False

def load_data(possible_paths: list) -> pd.DataFrame:
    """
    Загружает данные из первого найденного файла из списка путей.
    """
    for path in possible_paths:
        if os.path.exists(path):
            print(f"Загрузка данных из файла: {path}")
            try:
                df = pd.read_csv(path, low_memory=False)
                print(f"Данные загружены, размер: {df.shape}")
                return df
            except Exception as e:
                print(f"Ошибка при чтении файла {path}: {e}")
    print("Файл не найден ни по одному из путей.")
    sys.exit(1)

# --- 2. Очистка данных ---

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Удаляет строки с пропусками в valence и заполняет пропуски в genre.
    """
    print("\n=== Очистка данных ===")
    init_count = len(df)
    df_clean = df[df['valence'].notna()].copy()
    df_clean['genre'] = df_clean['genre'].fillna('Unknown')
    cleaned_count = len(df_clean)
    unique_genres = df_clean['genre'].nunique()
    print(f"Строк до очистки: {init_count}; после: {cleaned_count}")
    print(f"Уникальных жанров: {unique_genres}")
    return df_clean

# --- 3. Анализ данных: средняя valence по жанрам ---

def analyze_valence_by_genre(df: pd.DataFrame) -> pd.DataFrame:
    """
    Возвращает DataFrame с жанрами и средней valence, отсортированный по убыванию valence.
    """
    print("\n=== Анализ средней valence по жанрам ===")
    grouped = df.groupby('genre')['valence'].agg(['mean', 'count']).reset_index()
    grouped.columns = ['Genre', 'Mean_Valence', 'Count']
    grouped.sort_values('Mean_Valence', ascending=False, inplace=True)
    print(grouped.head(10).to_string(index=False))
    max_row = grouped.iloc[0]
    print(f"\nЖанр с максимальной средней valence: '{max_row['Genre']}'")
    print(f"Средняя valence: {max_row['Mean_Valence']:.3f}")
    print(f"Количество треков: {int(max_row['Count'])}")
    return grouped

# --- 4. Визуализация ---

def plot_top_genres(df_analysis: pd.DataFrame, top_n: int = 10) -> plt.Figure:
    """
    Строит горизонтальный barplot топ N жанров по средней valence.
    """
    sns.set_style('whitegrid')
    top_df = df_analysis.head(top_n)
    
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.barplot(x='Mean_Valence', y='Genre', data=top_df, palette='viridis_r', ax=ax)
    
    for bar in ax.patches:
        width = bar.get_width()
        ax.text(width + 0.01, bar.get_y() + bar.get_height() / 2, f'{width:.3f}', va='center', ha='left', fontsize=12)

    ax.set_title('Топ-10 жанров по средней valence (позитивности)', fontsize=16, pad=20)
    ax.set_xlabel('Средняя valence')
    ax.set_ylabel('Жанр')
    
    plt.tight_layout()
    plt.show()
    return fig

# --- 5. Сохранение графика в HDFS ---

def save_figure_to_hdfs(fig: plt.Figure, hdfs_path: str, hdfs_url: str = 'http://hadoop:9870', user: str = 'root'):
    """
    Сохраняет matplotlib figure в PNG формате в HDFS.
    """
    print(f"\nСохранение графика в HDFS: {hdfs_path}")
    buffer = io.BytesIO()
    fig.savefig(buffer, format='png', dpi=300)
    buffer.seek(0)

    client = InsecureClient(hdfs_url, user=user)
    client.makedirs(os.path.dirname(hdfs_path), exist_ok=True)
    
    with client.write(hdfs_path, overwrite=True) as writer:
        writer.write(buffer.getvalue())

    print("График успешно сохранён в HDFS.")

# --- Главная функция ---

def main():
    hdfs_file = "/user/hadoop/input/database.csv"
    local_file_paths = ["/opt/database.csv", "/opt/data/database.csv", "database.csv"]

    # Пытаемся скачать из HDFS
    downloaded = download_file_from_hdfs(hdfs_file, local_file_paths[0])

    # Загружаем из доступного локального файла
    df = load_data(local_file_paths)

    # Очистка
    df_clean = clean_data(df)

    # Анализ
    df_analysis = analyze_valence_by_genre(df_clean)

    # Визуализация
    fig = plot_top_genres(df_analysis, top_n=10)

    # Сохранение в HDFS
    output_hdfs_path = '/user/hadoop/results/valence_by_genre.png'
    save_figure_to_hdfs(fig, output_hdfs_path)

    # Проверка содержимого результатов в HDFS
    print("\nСодержимое /user/hadoop/results в HDFS:")
    subprocess.run("hdfs dfs -ls /user/hadoop/results", shell=True)

if __name__ == '__main__':
    main()
