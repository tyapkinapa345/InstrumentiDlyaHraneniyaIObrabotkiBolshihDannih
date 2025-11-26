# Удаляем из значений колонки valence все точки с запятой и пробелы, приводим к числу, пропуски в NaN
df_clean['valence'] = df_clean['valence'].str.replace(';', '').str.strip()
df_clean['valence'] = pd.to_numeric(df_clean['valence'], errors='coerce')

# Заполняем пропуски в valence средним значением (или другим подходящим)
df_clean['valence'] = df_clean['valence'].fillna(df_clean['valence'].mean())

# Для колонки genre удаляем лишние пробелы и символы
df_clean['genre'] = df_clean['genre'].str.strip().str.replace(';', '').str.lower()
df_clean['genre'] = df_clean['genre'].fillna('unknown')

# Теперь можно без проблем группировать по жанру и считать среднее valence
magnitude_by_type = df_clean.groupby('genre')['valence'].agg(['mean', 'count']).reset_index()
magnitude_by_type.columns = ['Genre', 'Mean_Valence', 'Count']
magnitude_by_type = magnitude_by_type.sort_values('Mean_Valence', ascending=False)

print(magnitude_by_type)
