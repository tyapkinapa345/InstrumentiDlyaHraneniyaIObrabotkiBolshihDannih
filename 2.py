Ниже приведена подробная структура кода по вашим блокам с учётом данных землетрясений, с загрузкой из HDFS, очисткой, анализом и визуализацией с сохранением графика в HDFS.

***

### 1. Загрузка данных из HDFS и локальная проверка

```python
import pandas as pd
import os
import subprocess

print("Загрузка данных из HDFS...")

hdfs_path = "/user/hadoop/input/database.csv"
local_path = "/opt/database.csv"

try:
    hdfs_download_cmd = f"hdfs dfs -get {hdfs_path} {local_path}"
    print(f"Выполнение команды: {hdfs_download_cmd}")

    env = dict(os.environ, **{'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-amd64'})
    result = subprocess.run(hdfs_download_cmd, shell=True, capture_output=True, text=True, cwd="/opt", env=env)

    if result.returncode == 0:
        print(f"Данные успешно загружены из HDFS: {hdfs_path}")
    else:
        print(f"Ошибка при загрузке из HDFS: {result.stderr}")
        print("Попытка найти файл локально...")
        local_path = "/opt/data/database.csv"

    if not os.path.exists(local_path):
        print(f"Файл не найден в {local_path}. Используем альтернативный путь...")
        local_path = "database.csv"

except Exception as e:
    print(f"Ошибка при выполнении subprocess: {e}")
    print("Попытка использовать локальный файл...")
    local_path = "/opt/data/database.csv"

if not os.path.exists(local_path):
    print("Файл не найден. Пробуем последний вариант...")
    local_path = "database.csv"

if os.path.exists(local_path):
    df = pd.read_csv(local_path, low_memory=False)
    print(f"Размер датасета: {df.shape}")
    print(f"Данные успешно загружены из {local_path}")
    print(df.head())
else:
    print(f"ОШИБКА: Файл database.csv не найден!")
    df = pd.DataFrame()
```

***

### 2. Очистка данных

```python
df_clean = df.copy()
df_clean = df_clean[df_clean['Magnitude'].notna()]
df_clean['Type'] = df_clean['Type'].fillna('Unknown')

print(f"Количество строк после очистки: {len(df_clean)}")
print(f"Типы землетрясений: {df_clean['Type'].unique()}")
```

***

### 3. Анализ средней магнитуды по типам

```python
magnitude_by_type = df_clean.groupby('Type')['Magnitude'].agg(['mean', 'count']).reset_index()
magnitude_by_type.columns = ['Type', 'Mean_Magnitude', 'Count']
magnitude_by_type = magnitude_by_type.sort_values('Mean_Magnitude', ascending=False)

print("Средняя магнитуда по типам:")
print(magnitude_by_type)

max_type = magnitude_by_type.iloc[0]
print(f"Тип с максимальной средней магнитудой: {max_type['Type']}")
print(f"Средняя магнитуда: {max_type['Mean_Magnitude']:.2f}")
print(f"Количество землетрясений: {int(max_type['Count'])}")
```

***

### 4. Визуализация с сохранением результата в HDFS

```python
import io
import matplotlib.pyplot as plt
import seaborn as sns
from hdfs import InsecureClient

plt.style.use('seaborn-v0_8-whitegrid')
plt.figure(figsize=(12, 7))
ax = sns.barplot(
    x='Mean_Magnitude',
    y='Type',
    data=magnitude_by_type.head(10),
    palette='viridis_r'
)

min_val = magnitude_by_type['Mean_Magnitude'].min()
plt.xlim(left=min_val - 0.1)

for bar in ax.patches:
    ax.text(
        bar.get_width() + 0.01,
        bar.get_y() + bar.get_height() / 2,
        f'{bar.get_width():.2f}',
        va='center', ha='left',
        fontsize=12, color='black'
    )

plt.title('Топ-10 типов землетрясений по средней магнитуде', fontsize=16, pad=20)
plt.xlabel('Средняя магнитуда (Mw)', fontsize=12)
plt.ylabel('Тип землетрясения', fontsize=12)
sns.despine(left=True, bottom=True)
plt.tight_layout()

buffer = io.BytesIO()
plt.savefig(buffer, format='png', dpi=300)
plt.show()
buffer.seek(0)

hdfs_path = '/user/hadoop/results/magnitude_by_type.png'
client = InsecureClient('http://hadoop:9870', user='root')

client.makedirs('/user/hadoop/results')
with client.write(hdfs_path, overwrite=True) as writer:
    writer.write(buffer.getvalue())

print(f"График успешно сохранен в HDFS по пути: {hdfs_path}")

# Проверка
!hdfs dfs -ls /user/hadoop/results
```

