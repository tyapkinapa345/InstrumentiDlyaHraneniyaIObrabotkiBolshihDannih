import io
import os
import matplotlib.pyplot as plt
from hdfs import InsecureClient

# --- 1. Построение графика ---
plt.figure(figsize=(12, 8))
top_10 = magnitude_by_type.head(10)
plt.barh(top_10['Genre'], top_10['Mean_Valence'])
plt.xlabel('Средняя valence (позитивность)')
plt.ylabel('Жанр')
plt.title('Топ-10 жанров по средней valence')
plt.tight_layout()

# --- 2. Сохранение графика в буфер памяти ---
buffer = io.BytesIO()
plt.savefig(buffer, format='png', dpi=300)
plt.show()
buffer.seek(0)

# --- 3. Подключение к HDFS и запись файла ---
hdfs_path = '/user/hadoop/results/valence_by_genre.png'
hdfs_dir = os.path.dirname(hdfs_path)

client = InsecureClient('http://hadoop:9870', user='root')
client.makedirs(hdfs_dir)

with client.write(hdfs_path, overwrite=True) as writer:
    writer.write(buffer.getvalue())

print(f"График успешно сохранён в HDFS по пути: {hdfs_path}")

# --- 4. Проверка результата ---
os.system('hdfs dfs -ls /user/hadoop/results')









import io
import os
import matplotlib.pyplot as plt
import seaborn as sns
from hdfs import InsecureClient

# --- 1. Подготовка данных и сортировка ---
# Предполагается, что у вас уже есть DataFrame 'magnitude_by_type'
df_sorted = magnitude_by_type.sort_values('Mean_Magnitude', ascending=False)

# --- 2. Создание улучшенного графика с seaborn ---
plt.style.use('seaborn-v0_8-whitegrid')
plt.figure(figsize=(12, 7))

ax = sns.barplot(
    x='Mean_Magnitude',
    y='Type',
    data=df_sorted,
    palette='viridis_r'
)

min_val = df_sorted['Mean_Magnitude'].min()
plt.xlim(left=min_val - 0.1)

for bar in ax.patches:
    ax.text(
        bar.get_width() + 0.01,
        bar.get_y() + bar.get_height() / 2,
        f'{bar.get_width():.2f}',
        va='center', ha='left',
        fontsize=12, color='black'
    )

plt.title('Сравнение средней магнитуды по типам событий', fontsize=16, pad=20)
plt.xlabel('Средняя магнитуда (Mw)', fontsize=12)
plt.ylabel('Тип события', fontsize=12)
sns.despine(left=True, bottom=True)
plt.tight_layout()

# --- 3. Сохранение графика в буфер памяти ---
buffer = io.BytesIO()
plt.savefig(buffer, format='png', dpi=300)
plt.show()
buffer.seek(0)

# --- 4. Подключение к HDFS и запись файла ---
hdfs_path = '/user/hadoop/results/magnitude_by_type.png'
client = InsecureClient('http://hadoop:9870', user='root')

client.makedirs(os.path.dirname(hdfs_path))
with client.write(hdfs_path, overwrite=True) as writer:
    writer.write(buffer.getvalue())

print(f"График успешно перезаписан в HDFS по пути: {hdfs_path}")

# --- 5. Проверка результата ---
os.system('hdfs dfs -ls /user/hadoop/results')
