import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import subprocess
import os
import io
from hdfs import InsecureClient

# Настройка отображения
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")
# Для Jupyter можно оставлять эту строчку:
# %matplotlib inline

# Увеличение размера графиков
plt.rcParams['figure.figsize'] = (12, 8)

import pandas as pd
import os
import subprocess

print("Загрузка данных из HDFS ...")

hdfs_path = "/user/hadoop/input2/database.csv"
local_path = "/opt/database.csv"

print(os.listdir("/opt"))

try:
    # Удаляем локальный файл, если он существует (чтобы избежать ошибки "File exists")
    if os.path.exists(local_path):
        os.remove(local_path)

    hdfs_download_cmd = f"hdfs dfs -get {hdfs_path} {local_path}"
    print(f"Выполнение команды: {hdfs_download_cmd}")

    env = dict(os.environ, **{'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-amd64'})
    result = subprocess.run(hdfs_download_cmd, shell=True, capture_output=True, text=True, cwd="/opt", env=env)

    if result.returncode == 0:
        print(f"Данные успешно загружены из HDFS: {hdfs_path}")
    else:
        print(f"Ошибка при загрузке из HDFS: {result.stderr}")
        print("Попытка найти файл локально ...")
        local_path = "/opt/data/database.csv"

    if not os.path.exists(local_path):
        print(f"Файл не найден в {local_path}. Используем альтернативный путь ...")
        local_path = "database.csv"

except Exception as e:
    print(f"Ошибка при выполнении subprocess: {e}")
    print("Попытка использовать локальный файл ...")
    local_path = "/opt/data/database.csv"

if not os.path.exists(local_path):
    print("Файл не найден. Пробуем последний вариант ...")
    local_path = "database.csv"

if os.path.exists(local_path):
    df = pd.read_csv(local_path, encoding='utf-8-sig', sep=',', quotechar='"', engine='python', on_bad_lines='skip')
    print(f"Размер датасета: {df.shape}")
    print(f"Данные успешно загружены из {local_path}")
    print(df.head())
else:
    print("ОШИБКА: Файл database.csv не найден!")
    print("Искали по следующим путям:")
    print(" - /opt/database.csv (из HDFS)")
    print(" - /opt/data/database.csv (локальный)")
    print(" - database.csv (в текущей директории)")
    df = pd.DataFrame()


# Начинаем очистку данных под Spotify датасет

df_clean = df.copy()

# Вместо 'Magnitude' теперь проверяем 'valence' (показатель позитивности трека)
df_clean = df_clean[df_clean['valence'].notna()]

# Вместо 'Type' - это 'genre', пропуски заполняем 'Unknown'
df_clean['genre'] = df_clean['genre'].fillna('Unknown')

print(f"Количество строк после очистки: {len(df_clean)}")
print(f"Жанры в данных: {df_clean['genre'].unique()}")

# Анализ valence по genre
magnitude_by_type = df_clean.groupby('genre')['valence'].agg(['mean', 'count']).reset_index()
magnitude_by_type.columns = ['Genre', 'Mean_Valence', 'Count']
magnitude_by_type = magnitude_by_type.sort_values('Mean_Valence', ascending=False)

print("Средняя valence по жанрам:")
print(magnitude_by_type)

max_type = magnitude_by_type.iloc[0]
print(f"Жанр с максимальной средней valence: {max_type['Genre']}")
print(f"Средняя valence: {max_type['Mean_Valence']:.3f}")
print(f"Количество треков: {int(max_type['Count'])}")

# Визуализация средней valence по жанрам (топ-10)
plt.figure(figsize=(12, 8))
top_10 = magnitude_by_type.head(10)
plt.barh(top_10['Genre'], top_10['Mean_Valence'])
plt.xlabel('Средняя valence')
plt.ylabel('Жанр')
plt.title('Топ-10 жанров по средней valence')
plt.tight_layout()
plt.show()

# Сохраняем график в буфер памяти
buffer = io.BytesIO()
plt.savefig(buffer, format='png', dpi=300)
plt.show()  # Чтобы график отобразился в ячейке
buffer.seek(0)

# Подключаемся к HDFS и записываем файл
hdfs_path = '/user/hadoop/results/valence_by_genre.png'
hdfs_dir = os.path.dirname(hdfs_path)
client = InsecureClient('http://hadoop:9870', user='root')
client.makedirs(hdfs_dir)
with client.write(hdfs_path, overwrite=True) as writer:
    writer.write(buffer.getvalue())

print(f"График успешно сохранен и/или перезаписан в HDFS по пути: {hdfs_path}")

# Проверяем содержимое директории в HDFS
subprocess.run("hdfs dfs -ls /user/hadoop/results", shell=True)

# Улучшенная визуализация с seaborn

import seaborn as sns

df_sorted = magnitude_by_type.sort_values('Mean_Valence', ascending=False)

plt.style.use('seaborn-v0_8-whitegrid')
plt.figure(figsize=(12, 7))

ax = sns.barplot(
    x='Mean_Valence',
    y='Genre',
    data=df_sorted,
    palette='viridis_r'
)

min_val = df_sorted['Mean_Valence'].min()
plt.xlim(left=min_val - 0.1)

for bar in ax.patches:
    ax.text(
        bar.get_width() + 0.01,
        bar.get_y() + bar.get_height() / 2,
        f'{bar.get_width():.3f}',
        va='center', ha='left',
        fontsize=12, color='black'
    )

plt.title('Сравнение средней valence по жанрам', fontsize=16, pad=20)
plt.xlabel('Средняя valence')
plt.ylabel('Жанр')
sns.despine(left=True, bottom=True)
plt.tight_layout()

buffer = io.BytesIO()
plt.savefig(buffer, format='png', dpi=300)
plt.show()
buffer.seek(0)

# Сохраняем обновленный график
with client.write(hdfs_path, overwrite=True) as writer:
    writer.write(buffer.getvalue())

print(f"График успешно перезаписан в HDFS по пути: {hdfs_path}")

subprocess.run("hdfs dfs -ls /user/hadoop/results", shell=True)
