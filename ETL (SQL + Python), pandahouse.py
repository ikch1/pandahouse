#!/usr/bin/env python
# coding: utf-8

# In[66]:


#импортируем нужные либы
import pandahouse as ph
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt


# In[2]:


#объявляем параметры подключения
connection = {'host': 'http://clickhouse.beslan.pro:8080',
                      'database':'default',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }


# Для начала, проверим правильность подключения к ClickHouse через pandahouse, отправив простой запрос: выведите количество строк в таблице ldn_listings.

# In[24]:


#пишем запрос, и получаем данные из clickhouse в pandas dataframe
query = """
SELECT room_type
FROM default.ldn_listings
"""
df = ph.read_clickhouse(query, connection=connection)
df


# Выгрузите из таблицы данные о цене в зависимости от типа жилья. Необходимые столбцы:
# 
# price – цена за ночь
# room_type – тип сдаваемого жилья (доступные варианты: Entire home/apt, Private room, Hotel room, Shared room)
# 
# Ограничение поставьте на 1000 строк. Результат должен быть отсортирован по возрастанию id.
# 
# Сгруппируйте полученный датафрейм по типу жилья и посчитайте 75-й перцентиль цены.
# В качестве ответа впишите полученное значение 75 перцентиля цены для комнат типа Private room.

# In[25]:


#пишем запрос, и получаем данные из clickhouse в pandas dataframe
query = """
SELECT 
toFloat32OrNull(replaceRegexpAll( price, '[,$]', '')) as price ,
room_type
FROM default.ldn_listings
ORDER BY id ASC
LIMIT 1000
"""
df = ph.read_clickhouse(query, connection=connection)
df


# In[26]:


df.groupby('room_type',as_index = False).agg({'price':sum}).sort_values('price', ascending = False)


# In[22]:


df.query('room_type == "Private room"').quantile(0.75)


# К данным о цене и типе комнаты дополнительно выгрузите данные о рейтинге жилья (review_scores_rating). В запросе необходимо будет отфильтровать пустые значения review_scores_rating и сконвертировать эту колонку в тип float32.
# 
# Давайте построим график рассеивания, который покажет зависимость средней оценки от средней цены по типу жилья.

# In[30]:


#пишем запрос, и получаем данные из clickhouse в pandas dataframe
query2 = """
SELECT 
toFloat32OrNull(replaceRegexpAll( price, '[,$]', '')) as price,
toFloat32OrNull(review_scores_rating) as scores,
room_type as room_type
FROM default.ldn_listings
WHERE review_scores_rating != ''
ORDER BY id ASC
LIMIT 1000
"""
df2 = ph.read_clickhouse(query2, connection=connection)
df2


# In[33]:


#группируем датафрейм по типу жилья и считаем среднее для цены и рейтинга

df2 = df2.groupby('room_type', as_index = False).agg({'price': 'mean', 'scores':'mean'})


# In[37]:


#используем sns.scatterplot, чтобы построить график рассеивания средней цены (ось X) и рейтинга (ось Y) c разбивкой по типу жилья (параметр hue)

sns.scatterplot(data = df2, x = 'price', y = 'scores', hue = 'room_type')


# Итак, помимо аренды жилья, на Airbnb также есть "Впечатления" — мероприятия, которые организуют местные жители.
# 
# Проверим, какие способы верификации аккаунта использовали хозяева, предлагающие различные впечатления (experiences_offered != 'none'). Для каждого уникального пользователя выгрузите только две колонки:
# 
# host_id – идентификатор хозяина (уникальный)
# host_verifications – какими способами хост подтвердил свой профиль

# In[50]:


#пишем запрос, и получаем данные из clickhouse в pandas dataframe
query3 = """
SELECT 
distinct host_id  ,
host_verifications 

FROM default.ldn_listings
WHERE experiences_offered != 'none'

"""
df3 = ph.read_clickhouse(query3, connection=connection)
df3


# In[51]:


#в ячейках находятся строковые представления списка. приводим их к настоящему списку, где в качестве элементов будут храниться использованные способы подтверждения аккаунта 

df3['host_verifications'] = df3['host_verifications'].apply(lambda x: x.strip().strip('[').strip(']').replace("'","").split(','))


# In[53]:


#используем методы explode и value_counts, чтобы посчитать, сколько раз встречается каждый способ верификации
df3.explode('host_verifications')['host_verifications'].value_counts()


# Теперь посмотрим, для скольких объявлений и в каких районах хозяева указали впечатления. Сгруппируйте данные по району и виду впечатления и посчитайте количество объявлений. Новый столбец назовите experiences_count

# In[13]:


#пишем запрос, и получаем данные из clickhouse в pandas dataframe
query4 = """
SELECT 
neighbourhood_cleansed,
experiences_offered,
count(id) as experiences_count 

FROM default.ldn_listings
WHERE experiences_offered != 'none'
GROUP BY neighbourhood_cleansed, experiences_offered
ORDER BY experiences_count DESC
LIMIT 100

"""
df4 = ph.read_clickhouse(query4, connection=connection)
df4


# In[54]:


#визуализируем пивот (индексы название района, столбцы – вид впечатления, а значения – число объявлений с таким впечатлением для каждого района)

sns.heatmap(df4.pivot(index = 'neighbourhood_cleansed', columns = 'experiences_offered', values = 'experiences_count'), cmap=sns.cubehelix_palette(as_cmap=True))


# Выгрузите данные о ценах за ночь для разных типов жилья, для которых также доступен какой-либо вид впечатления. Необходимые для составления запроса столбцы:
# 
# room_type – тип сдаваемого жилья (доступные варианты: Entire home/apt, Private room, Hotel room, Shared room)
# price – цена за ночь 
# experiences_offered – вид доступного впечатления (оставить не 'none')

# In[55]:


query5 = """
SELECT 
room_type,
toFloat32OrNull(replaceRegexpAll( price, '[,$]', '')) as price 
FROM default.ldn_listings
WHERE experiences_offered != 'none'

"""
df5 = ph.read_clickhouse(query5, connection=connection)
df5


# In[16]:


#отображаю исходные распределения цен для каждого типа жилья

sns.distplot(df5.query("room_type=='Private room'").price, kde=False, label='Private room')
sns.distplot(df5.query("room_type=='Entire home/apt'").price, kde=False, label='Entire home/apt')
sns.distplot(df5.query("room_type=='Shared room'").price, kde=False, label='Shared room')
sns.distplot(df5.query("room_type=='Hotel room'").price, kde=False, label='Hotel room')
plt.legend()
plt.show()


# In[56]:


#отображаю исходные распределения цен для каждого типа жилья, логарифмированные значения (np.log()

sns.distplot(np.log(df5.query("room_type=='Private room'").price), kde=False, label='type_1')
sns.distplot(np.log(df5.query("room_type=='Entire home/apt'").price), kde=False, label='type_2')
sns.distplot(np.log(df5.query("room_type=='Shared room'").price), kde=False, label='type_2')
sns.distplot(np.log(df5.query("room_type=='Hotel room'").price), kde=False, label='type_2')
plt.legend()
plt.show()


# Выгрузите данные о цене, типе жилья и дате первого отзыва, начиная со 2 января 2010 года. Необходимые столбцы:
# 
# room_type – тип сдаваемого жилья (доступные варианты: Entire home/apt, Private room, Hotel room, Shared room)
# price – цена за ночь
# first_review – дата первого отзыва (отфильтровать по правилу "строго больше 2010-01-01")
# Ограничение поставьте на 1000 строк.

# In[60]:


query6 = """
SELECT 
room_type,
toFloat32OrNull(replaceRegexpAll( price, '[,$]', '')) as price,
first_review
FROM default.ldn_listings
WHERE first_review > '2010-01-01'

LIMIT 1000

"""
df6 = ph.read_clickhouse(query6, connection=connection)
df6


# In[61]:


#приводим к типу даты
df6['first_review'] = pd.to_datetime(df6.first_review)


# In[63]:


#приводим к типу даты = год
df6['first_review'] = pd.DatetimeIndex(df6.first_review).year


# In[64]:


df6


# In[65]:


#строю график динамики средних цен на жилье (ось Y) в зависимости от типа комнаты (цвет линии, параметр 'hue') по годам (ось X)

plt.figure(figsize=(12, 8))
sns.lineplot(x='first_review',y='price',hue='room_type',data=df6)

