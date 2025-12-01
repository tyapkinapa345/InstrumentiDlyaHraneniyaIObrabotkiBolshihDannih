Вот адаптированный код для визуализации средних показателей **"energy" (энергичность)** и **"danceability" (танцевальность)** по жанрам с использованием двух графиков или комбинированной визуализации:

## Вариант 1: Два отдельных графика (рекомендуется)

```python
import io
import os
import matplotlib.pyplot as plt
import seaborn as sns
from hdfs import InsecureClient

# 1. Подготовка данных
df_clean['energy'] = pd.to_numeric(df_clean['energy'].astype(str).str.replace(';', ''), errors='coerce')
df_clean['danceability'] = pd.to_numeric(df_clean['danceability'].astype(str).str.replace(';', ''), errors='coerce')

# Группировка по жанру
energy_by_genre = df_clean.groupby('genre')['energy'].agg(['mean', 'count']).reset_index()
energy_by_genre.columns = ['Genre', 'Mean_Energy', 'Count']
energy_by_genre = energy_by_genre.sort_values('Mean_Energy', ascending=False)

dance_by_genre = df_clean.groupby('genre')['danceability'].agg(['mean', 'count']).reset_index()
dance_by_genre.columns = ['Genre', 'Mean_Danceability', 'Count']
dance_by_genre = dance_by_genre.sort_values('Mean_Danceability', ascending=False)

# 2. Создание двух графиков
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))

# График энергичности
sns.barplot(x='Mean_Energy', y='Genre', data=energy_by_genre.head(10), 
            palette='plasma', ax=ax1)
ax1.set_title('Топ-10 жанров по средней энергичности', fontsize=14, pad=10)
ax1.set_xlabel('Средняя энергичность', fontsize=12)
for bar in ax1.patches:
    ax1.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
             f'{bar.get_width():.3f}', va='center', ha='left', fontsize=10)

# График танцевальности
sns.barplot(x='Mean_Danceability', y='Genre', data=dance_by_genre.head(10), 
            palette='viridis', ax=ax2)
ax2.set_title('Топ-10 жанров по танцевальности', fontsize=14, pad=10)
ax2.set_xlabel('Средняя танцевальность', fontsize=12)
for bar in ax2.patches:
    ax2.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2, 
             f'{bar.get_width():.3f}', va='center', ha='left', fontsize=10)

plt.tight_layout()

# 3. Сохранение в буфер и HDFS
buffer = io.BytesIO()
plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
plt.show()
buffer.seek(0)

hdfs_path = '/user/hadoop/results/energy_danceability_by_genre.png'
client = InsecureClient('http://hadoop:9870', user='root')
client.makedirs(os.path.dirname(hdfs_path))
with client.write(hdfs_path, overwrite=True) as writer:
    writer.write(buffer.getvalue())

print(f"График сохранён в HDFS: {hdfs_path}")
```

## Вариант 2: Комбинированный график (сравнение в одном графике)

```python
# Подготовка данных для сравнения (топ-10 жанров)
top_genres_energy = energy_by_genre.head(10)
top_genres_dance = dance_by_genre.head(10)

# Объединяем данные для сравнения
comparison_data = pd.DataFrame({
    'Genre': top_genres_energy['Genre'],
    'Energy': top_genres_energy['Mean_Energy'],
    'Danceability': top_genres_dance.set_index('Genre').loc[top_genres_energy['Genre'], 'Mean_Danceability']
})

# График сравнения
plt.figure(figsize=(14, 8))
x = np.arange(len(comparison_data))
width = 0.35

plt.bar(x - width/2, comparison_data['Energy'], width, label='Энергичность', alpha=0.8)
plt.bar(x + width/2, comparison_data['Danceability'], width, label='Танцевальность', alpha=0.8)

plt.xlabel('Жанры')
plt.ylabel('Средние показатели')
plt.title('Сравнение энергичности и танцевальности по жанрам')
plt.xticks(x, comparison_data['Genre'], rotation=45, ha='right')
plt.legend()
plt.tight_layout()

# Сохранение
buffer = io.BytesIO()
plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
plt.show()
buffer.seek(0)

hdfs_path = '/user/hadoop/results/energy_vs_danceability.png'
client = InsecureClient('http://hadoop:9870', user='root')
client.makedirs(os.path.dirname(hdfs_path))
with client.write(hdfs_path, overwrite=True) as writer:
    writer.write(buffer.getvalue())

print(f"Сравнительный график сохранён: {hdfs_path}")
```

## Вывод результатов в таблицу

```python
print("=== ТОП-5 ЖАНРОВ ПО ЭНЕРГИЧНОСТИ ===")
print(energy_by_genre.head().round(3)[['Genre', 'Mean_Energy']])
print("\n=== ТОП-5 ЖАНРОВ ПО ТАНЦЕВАННОСТИ ===")
print(dance_by_genre.head().round(3)[['Genre', 'Mean_Danceability']])
```

Оба варианта дают наглядное представление о том, какие жанры наиболее энергичные и танцевальные, с сохранением результатов в HDFS для дальнейшего использования.
