Да, очистить данные внутри столбцов в pandas можно с помощью разных приёмов. Вот несколько распространённых методов очистки данных внутри столбцов:

1. Удаление лишних пробелов и спецсимволов из текстовых данных:
```python
df['genre'] = df['genre'].str.strip()  # удалить пробелы в начале и конце строки
df['genre'] = df['genre'].str.replace(';', '')  # удалить лишние символы, как точка с запятой
```

2. Приведение текста к одному регистру для унификации:
```python
df['genre'] = df['genre'].str.lower()  # все буквы в нижний регистр
```

3. Заполнение пропущенных значений:
```python
df['genre'] = df['genre'].fillna('unknown')  # заполнить пропуски значением 'unknown'
```

4. Преобразование числовых данных с очисткой и заменой:
```python
df['valence'] = pd.to_numeric(df['valence'], errors='coerce')  # попытка преобразовать в число, ошибки в NaN
df['valence'] = df['valence'].fillna(df['valence'].mean())  # заполнение NaN средним значением столбца
```

5. Удаление строк с пустыми или ошибочными значениями:
```python
df = df.dropna(subset=['valence', 'genre'])
```

6. Удаление дубликатов (строк с идентичными данными):
```python
df = df.drop_duplicates()
```

Вот пример объединения нескольких приёмов для ваших колонок:

```python
df['valence'] = pd.to_numeric(df['valence'].str.replace(';', ''), errors='coerce')
df['valence'] = df['valence'].fillna(df['valence'].mean())

df['genre'] = df['genre'].str.strip().str.replace(';', '').str.lower()
df['genre'] = df['genre'].fillna('unknown')

df = df.dropna(subset=['valence', 'genre'])
df = df.drop_duplicates()
```

Так вы приведёте данные к более чистому и удобному для анализа виду. При необходимости можно добавить более специфичные трансформации для вашего набора данных.

[1](https://django.fun/articles/python/ochistka-dannyh-s-pomoshyu-pandas-i-numpy/)
[2](https://habr.com/ru/companies/otus/articles/797283/)
[3](https://nerdit.ru/mietody-ochistki-dannykh-v-pandas/)
[4](https://proglib.io/p/moem-dataset-rukovodstvo-po-ochistke-dannyh-v-python-2020-03-27)
[5](https://is42-2018.susu.ru/shvyrkovmi/2021/06/05/ochistka-dannyh-s-pomoshhju-pandas-i-numpy/)
[6](https://ru.hexlet.io/qna/data-analytics/questions/kak-udalit-datafreym-pandas)
[7](https://arenda-server.cloud/blog/pandas-dropna-udalenie-pustyh-i-na-znachenij-iz-dataframe/)
[8](https://labex.io/ru/tutorials/pandas-pandas-basic-data-cleaning-596390)
[9](https://dfedorov.spb.ru/pandas/%D0%9E%D1%87%D0%B8%D1%81%D1%82%D0%BA%D0%B0%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85%20%D0%BE%20%D0%B2%D0%B0%D0%BB%D1%8E%D1%82%D0%B5%20%D1%81%20%D0%BF%D0%BE%D0%BC%D0%BE%D1%89%D1%8C%D1%8E%20pandas.html)
[10](https://sql-ex.ru/blogs/?%2FKak_uluchshit_analiz_dannyh_jeffektivnaJa_chistka_dannyh_s_pomowju_Python.html)
