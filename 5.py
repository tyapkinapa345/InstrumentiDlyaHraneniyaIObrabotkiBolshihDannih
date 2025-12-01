Вот переработанные коды с использованием seaborn для более современного и стильного визуального оформления:

## 1. График средней valence по жанрам (усовершенствованный)

```python
import io
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from hdfs import InsecureClient

# Установка стиля seaborn
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = [12, 8]
plt.rcParams['font.size'] = 12

# Предполагаем, что magnitude_by_type уже существует
# Создаем топ-10 жанров
top_10 = magnitude_by_type.head(10).sort_values('Mean_Valence', ascending=True)

# Создаем график
fig, ax = plt.subplots(figsize=(14, 10))

# Используем barplot от seaborn
barplot = sns.barplot(
    data=top_10,
    x='Mean_Valence',
    y='Genre',
    palette='viridis',  # Можно использовать: 'rocket', 'mako', 'crest', 'flare'
    ax=ax,
    hue='Genre',  # Добавляем hue для использования палитры
    legend=False,  # Отключаем легенду
    saturation=0.85  # Насыщенность цветов
)

# Настройка внешнего вида
ax.set_xlabel('Средняя valence (позитивность)', fontsize=14, fontweight='bold')
ax.set_ylabel('Жанр', fontsize=14, fontweight='bold')
ax.set_title('Топ-10 жанров по средней valence', fontsize=16, fontweight='bold', pad=20)

# Добавляем значения на столбцы
for i, (index, row) in enumerate(top_10.iterrows()):
    ax.text(
        row['Mean_Valence'] + 0.01,  # Смещение от столбца
        i,  # Позиция по y
        f'{row["Mean_Valence"]:.3f}',  # Форматированное значение
        va='center',
        fontsize=12,
        fontweight='bold'
    )

# Улучшаем сетку
ax.xaxis.grid(True, linestyle='--', alpha=0.7)
ax.yaxis.grid(False)

# Убираем рамку
sns.despine(left=True, bottom=True)

plt.tight_layout()

# Сохранение в буфер и HDFS
buffer = io.BytesIO()
plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight', facecolor='white')
buffer.seek(0)

# Сохранение в HDFS
hdfs_path = '/user/hadoop/results/valence_by_genre_seaborn.png'
client = InsecureClient('http://hadoop:9870', user='root')
client.makedirs(os.path.dirname(hdfs_path), exist_ok=True)

with client.write(hdfs_path, overwrite=True) as writer:
    writer.write(buffer.getvalue())

print(f"График сохранён в HDFS: {hdfs_path}")
plt.show()
```

## 2. Комбинированный график энергичности и танцевальности

```python
import io
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from hdfs import InsecureClient

# Установка стиля
sns.set_style("whitegrid")
plt.rcParams['font.size'] = 11

# Подготовка данных (если еще не сделано)
if 'energy' in df_clean.columns and 'danceability' in df_clean.columns:
    # Преобразование строк в числа (если нужно)
    if df_clean['energy'].dtype == 'object':
        df_clean['energy'] = pd.to_numeric(
            df_clean['energy'].astype(str).str.replace(';', ''),
            errors='coerce'
        )
    if df_clean['danceability'].dtype == 'object':
        df_clean['danceability'] = pd.to_numeric(
            df_clean['danceability'].astype(str).str.replace(';', ''),
            errors='coerce'
        )

# Группировка и подготовка данных
def prepare_genre_data(df, column, n_top=10):
    """Подготовка данных по жанрам"""
    genre_stats = df.groupby('genre')[column].agg(['mean', 'count']).reset_index()
    genre_stats.columns = ['Genre', f'Mean_{column.capitalize()}', 'Count']
    genre_stats = genre_stats.sort_values(f'Mean_{column.capitalize()}', ascending=False)
    return genre_stats.head(n_top).sort_values(f'Mean_{column.capitalize()}', ascending=True)

energy_data = prepare_genre_data(df_clean, 'energy', 10)
dance_data = prepare_genre_data(df_clean, 'danceability', 10)

# Создание subplot
fig, axes = plt.subplots(1, 2, figsize=(20, 10))

# График 1: Энергичность
sns.barplot(
    data=energy_data,
    x='Mean_Energy',
    y='Genre',
    palette='rocket_r',  # Реверсивная палитра
    ax=axes[0],
    hue='Genre',
    legend=False,
    edgecolor='black',
    linewidth=0.5
)

axes[0].set_title('Топ-10 жанров по энергичности', fontsize=14, fontweight='bold', pad=15)
axes[0].set_xlabel('Средняя энергичность (0-1)', fontsize=12)
axes[0].set_ylabel('')
axes[0].set_xlim(0, 1)

# Добавление значений
for i, (index, row) in enumerate(energy_data.iterrows()):
    axes[0].text(
        row['Mean_Energy'] + 0.02,
        i,
        f'{row["Mean_Energy"]:.3f}',
        va='center',
        fontsize=10,
        fontweight='bold'
    )

# График 2: Танцевальность
sns.barplot(
    data=dance_data,
    x='Mean_Danceability',
    y='Genre',
    palette='crest',  # Другая палитра для контраста
    ax=axes[1],
    hue='Genre',
    legend=False,
    edgecolor='black',
    linewidth=0.5
)

axes[1].set_title('Топ-10 жанров по танцевальности', fontsize=14, fontweight='bold', pad=15)
axes[1].set_xlabel('Средняя танцевальность (0-1)', fontsize=12)
axes[1].set_ylabel('')
axes[1].set_xlim(0, 1)

# Добавление значений
for i, (index, row) in enumerate(dance_data.iterrows()):
    axes[1].text(
        row['Mean_Danceability'] + 0.02,
        i,
        f'{row["Mean_Danceability"]:.3f}',
        va='center',
        fontsize=10,
        fontweight='bold'
    )

# Общие настройки
for ax in axes:
    ax.xaxis.grid(True, linestyle='--', alpha=0.3)
    ax.yaxis.grid(False)
    sns.despine(ax=ax, left=True, bottom=True)

plt.suptitle('Сравнение музыкальных характеристик по жанрам', 
             fontsize=16, fontweight='bold', y=1.02)
plt.tight_layout()

# Сохранение
buffer = io.BytesIO()
plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
buffer.seek(0)

hdfs_path = '/user/hadoop/results/energy_danceability_comparison.png'
client = InsecureClient('http://hadoop:9870', user='root')
client.makedirs(os.path.dirname(hdfs_path), exist_ok=True)

with client.write(hdfs_path, overwrite=True) as writer:
    writer.write(buffer.getvalue())

print(f"График сохранён в HDFS: {hdfs_path}")
plt.show()

# Вывод статистики
print("\n" + "="*50)
print("СТАТИСТИКА ПО ХАРАКТЕРИСТИКАМ")
print("="*50)
print("\n📊 ТОП-5 ЖАНРОВ ПО ЭНЕРГИЧНОСТИ:")
print(energy_data[['Genre', 'Mean_Energy']].round(3).head().to_string(index=False))
print("\n💃 ТОП-5 ЖАНРОВ ПО ТАНЦЕВАЛЬНОСТИ:")
print(dance_data[['Genre', 'Mean_Danceability']].round(3).head().to_string(index=False))
```

## 3. Дополнительно: Heatmap корреляций

```python
import io
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from hdfs import InsecureClient

# Установка стиля
sns.set_style("white")
plt.rcParams['figure.figsize'] = [14, 10]

# Выбор числовых колонок для анализа
numeric_columns = ['valence', 'energy', 'danceability', 'acousticness', 
                   'instrumentalness', 'liveness', 'speechiness']

# Проверка наличия колонок
available_cols = [col for col in numeric_columns if col in df_clean.columns]

if len(available_cols) >= 2:
    # Преобразование в числовой формат
    for col in available_cols:
        if df_clean[col].dtype == 'object':
            df_clean[col] = pd.to_numeric(
                df_clean[col].astype(str).str.replace(';', ''),
                errors='coerce'
            )
    
    # Вычисление корреляционной матрицы
    correlation_matrix = df_clean[available_cols].corr()
    
    # Создание heatmap
    fig, ax = plt.subplots(figsize=(12, 10))
    
    mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
    
    sns.heatmap(
        correlation_matrix,
        mask=mask,
        annot=True,
        fmt='.2f',
        cmap='coolwarm',
        center=0,
        square=True,
        linewidths=1,
        cbar_kws={'shrink': 0.8},
        ax=ax
    )
    
    # Русские названия для осей
    russian_labels = {
        'valence': 'Позитивность',
        'energy': 'Энергичность',
        'danceability': 'Танцевальность',
        'acousticness': 'Акустичность',
        'instrumentalness': 'Инструментальность',
        'liveness': 'Живость',
        'speechiness': 'Речевость'
    }
    
    labels = [russian_labels.get(col, col) for col in available_cols]
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_yticklabels(labels, rotation=0)
    
    plt.title('Корреляция музыкальных характеристик', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()
    
    # Сохранение
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
    buffer.seek(0)
    
    hdfs_path = '/user/hadoop/results/correlation_heatmap.png'
    client = InsecureClient('http://hadoop:9870', user='root')
    client.makedirs(os.path.dirname(hdfs_path), exist_ok=True)
    
    with client.write(hdfs_path, overwrite=True) as writer:
        writer.write(buffer.getvalue())
    
    print(f"Heatmap сохранён в HDFS: {hdfs_path}")
    plt.show()
else:
    print("Недостаточно числовых колонок для построения heatmap")
```

## 4. Универсальная функция для сохранения в HDFS

```python
def save_plot_to_hdfs(fig, filename, subdirectory='results'):
    """Универсальная функция для сохранения графиков в HDFS"""
    import io
    from hdfs import InsecureClient
    
    # Сохранение в буфер
    buffer = io.BytesIO()
    fig.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
    buffer.seek(0)
    
    # Путь в HDFS
    hdfs_path = f'/user/hadoop/{subdirectory}/{filename}'
    
    # Подключение к HDFS и сохранение
    client = InsecureClient('http://hadoop:9870', user='root')
    client.makedirs(os.path.dirname(hdfs_path), exist_ok=True)
    
    with client.write(hdfs_path, overwrite=True) as writer:
        writer.write(buffer.getvalue())
    
    print(f"✅ График сохранён: {hdfs_path}")
    return hdfs_path

# Пример использования:
# fig = plt.figure() ... построение графика
# save_plot_to_hdfs(fig, 'my_plot.png')
```

**Преимущества переработанного кода:**

1. **Современный дизайн** - использование стилей seaborn
2. **Читаемость** - улучшенные шрифты и отступы
3. **Информативность** - добавление значений на столбцы
4. **Профессиональный вид** - использование сетки и рамок
5. **Гибкость** - возможность легко менять палитры
6. **Модульность** - разделение на функции для повторного использования
7. **Обработка ошибок** - проверка типов данных и наличие колонок

Все графики автоматически сохраняются в HDFS с понятными путями и названиями.
