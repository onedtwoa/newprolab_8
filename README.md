
## 0. Подготовка инфраструктуры

Подняли ClickHouse и Airflow.

---
## 1. Изучение данных

Начинаем с изучения данных и определения их структуры и связей.

### 1.1. Загрузка данных

```bash
curl "data_url" | docker run -i --rm --network=host yandex/clickhouse-client --query="INSERT INTO <table> FORMAT JSONEachRow" --user <test> --password <test>
```

**Пример:**
```bash
curl "https://raw.githubusercontent.com/.../sample/browser_events.jsonl" | \
docker run -i --rm --network=host yandex/clickhouse-client --query="INSERT INTO browser_events FORMAT JSONEachRow" --user test --password test
```
---
### 1.2. Анализ структуры данных

#### Таблица `browser_events`
- **`event_timestamp`** — время события
- **`event_type`** — тип события → только `pageview` - не интересно
- **`click_id`** — сессия? 

#### Таблица `device_events`
- **`click_id`** — сессия? 
- **`user_custom_id`** — понятно из названия
- **`user_domain_id`** — понятно из названия

#### Таблица `geo_events`
- **`click_id`** — сессия?
  
#### Таблица `location_events`
- **`event_type`** — тип события
- **`page_url`** — URL 
- **`referer_url`** — URL источника 

---

### 1.3. Объединение

Подтверждаем наши предположения о сессии
```sql
SELECT
    click_id,
    COUNT(event_id) AS events_per_click
FROM union_events
GROUP BY click_id
ORDER BY events_per_click DESC;
```

Объединяем таблицы по `click_id` м `event_id`:

```sql
SELECT
    be.event_id,
    be.event_timestamp,
    be.event_type,
    be.click_id,
    de.user_custom_id,
    be.browser_name,
    de.os_name,
    de.device_type,
    ge.geo_country,
    ge.geo_region_name,
    le.page_url,
    le.referer_url,
    le.utm_source,
    le.utm_medium,
    le.utm_campaign
FROM
    browser_events AS be
LEFT JOIN device_events AS de ON be.click_id = de.click_id
LEFT JOIN geo_events AS ge ON be.click_id = ge.click_id
LEFT JOIN location_events AS le ON be.event_id = le.event_id;
```

---

## 2. Определение события "покупка"

### 2.1. Анализ URL

```sql
SELECT DISTINCT(page_url) FROM union_events;
```
- Интересные страницы:
  - `http://www.dummywebsite.com/payment` (17k записей).
  - `http://www.dummywebsite.com/confirmation` (12k записей).

---

### 2.2. Выявление покупок

Предпологаем, что покупка состоит из двух последовательных шагов
1. Визит на страницу `payment`.
2. Визит на страницу `confirmation`.

```sql
SELECT
    click_id,
    MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/payment' THEN event_timestamp END) AS payment_time,
    MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/confirmation' THEN event_timestamp END) AS confirmation_time
FROM union_events
WHERE page_url IN ('http://www.dummywebsite.com/payment', 'http://www.dummywebsite.com/confirmation')
GROUP BY click_id
HAVING
    payment_time IS NOT NULL
    AND confirmation_time IS NOT NULL
    AND payment_time < confirmation_time;
```

---

### 2.4.  Подтверждаем нашу догадку и смотрим противоречия

Смотрим время между переход по ссылкам (всего 5 сделок больше 1 мин)

```sql
SELECT
    click_id,
    MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/payment' THEN event_timestamp END) AS payment_time,
    MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/confirmation' THEN event_timestamp END) AS confirmation_time,
    dateDiff('minute', 
             MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/payment' THEN event_timestamp END), 
             MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/confirmation' THEN event_timestamp END)) AS time_difference_minutes
FROM union_events
WHERE page_url IN ('http://www.dummywebsite.com/payment', 'http://www.dummywebsite.com/confirmation')
GROUP BY click_id
HAVING
    payment_time IS NOT NULL
    AND confirmation_time IS NOT NULL
    AND payment_time < confirmation_time
    AND time_difference_minutes > 0.1
ORDER BY time_difference_minutes DESC;
```

Вычисляем среднее время между оплатой и подтверждением (0.24 сек)

```sql
SELECT
    AVG(time_diff_minutes) AS avg_time_diff_minutes
FROM (
    SELECT
        click_id,
        dateDiff('minute', payment_time, confirmation_time) AS time_diff_minutes
    FROM (
        SELECT
            click_id,
            MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/payment' THEN event_timestamp END) AS payment_time,
            MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/confirmation' THEN event_timestamp END) AS confirmation_time
        FROM union_events
        WHERE page_url IN ('http://www.dummywebsite.com/payment', 'http://www.dummywebsite.com/confirmation')
        GROUP BY click_id
        HAVING
            payment_time IS NOT NULL
            AND confirmation_time IS NOT NULL
            AND payment_time < confirmation_time
    ) AS t
) AS diffs;
```

Ищем случаи, где подтверждение предшествует оплате (таких нет)

```sql
SELECT
    click_id,
    payment_time,
    confirmation_time
FROM (
    SELECT
        click_id,
        MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/payment' THEN event_timestamp END) AS payment_time,
        MIN(CASE WHEN page_url = 'http://www.dummywebsite.com/confirmation' THEN event_timestamp END) AS confirmation_time
    FROM union_events
    WHERE page_url IN ('http://www.dummywebsite.com/payment', 'http://www.dummywebsite.com/confirmation')
    GROUP BY click_id
    HAVING
        payment_time IS NOT NULL
        AND confirmation_time IS NOT NULL
        AND payment_time > confirmation_time
) AS anomalies;
```

## 3. Реальные данные 

### 3.1 [Даг](./dag_get_and_load_data_lab08.py)

```
clean_up_before_download() 
>> download_and_unzip 
>> delete_data_from_clickhouse() 
>> load_data_to_clickhouse()
```
#### 1. `clean_up_before_download`
- Проверяется наличие директории для текущей даты.  
- Если директория существует, её содержимое удаляется.  
- Создается новая пустая директория для хранения данных.

```python
date_str = execution_date.strftime('%Y-%m-%d')
date_path = os.path.join(download_path, date_str)

if os.path.exists(date_path):
    shutil.rmtree(date_path)
    logging.info(f"Директория {date_path} очищена перед загрузкой новых данных.")
os.makedirs(date_path, exist_ok=True)
```
#### 2. `download_and_unzip`
- Используем `boto3`.
- Ищем файлы формата `.jsonl.zip` в хранилище по заданной дате.
- Формируем список найденных файлов.
- Для каждого файла:
  - Скачиваем его в локальную директорию.
  - Распаковываем содержимое архива.
  - Удаляем оригинальный архив после успешной распаковки.

```python
s3_client = boto3.client(
    's3',
    endpoint_url=endpoint_url,
    config=Config(signature_version=UNSIGNED)
)

for file_info in files:
    key = file_info['key']
    local_path = os.path.join(date_path, os.path.basename(key))
    s3_client.download_file(bucket_name, key, local_path)
    logging.info(f"Файл {key} скачан в {local_path}")

    if local_path.endswith('.zip'):
        with zipfile.ZipFile(local_path, 'r') as zip_ref:
            zip_ref.extractall(date_path)
        os.remove(local_path)
        logging.info(f"Архив {local_path} удален после распаковки.")
```

#### 3. `delete_data_from_clickhouse`
- Чистим таблицы за текущую даты, на случай перезаписи данных.  
(После чистки browser_events, каскадно удаляем данные из остальных таблиц и мат. представления)
- Это предотвращает дублирование данных при повторных запусках.
```python
query = f"ALTER TABLE {table} DELETE WHERE toDate(event_timestamp) = '{date_str}'"
client_clickhouse.execute(query)
logging.info(f"Данные из таблицы {table} за {date_str} успешно удалены.")
```

#### 4. `load_data_to_clickhouse`
- Загружаем данные в ClickHouse с использованием команды `INSERT INTO ... FORMAT JSONEachRow`.

```python
if 'browser_events' in jsonl_file:
    table_name = 'browser_events'
elif 'device_events' in jsonl_file:
    table_name = 'device_events'
# ...

with open(jsonl_file, 'r') as f:
    data = f.read()
if data:
    client_clickhouse.execute(
        f"INSERT INTO {table_name} FORMAT JSONEachRow {data}"
    )
    logging.info(f"Данные из файла {jsonl_file} загружены в таблицу {table_name}.")
```

## 4. Финальные шаги подготовки данных и создание дашборда

Проверяем еще раз на частично загруженных боевых данных. Затем переходим к созданию вьюх, которые объединили все необходимые данные в единый источник для аналитики.

---

### 4.1. Создание материализованного представления `mv_union_events` на `основе union_events`

```sql
CREATE MATERIALIZED VIEW mv_union_events
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time)
AS
SELECT
    be.event_id AS event_id,
    be.event_timestamp AS event_time,
    be.event_type AS event_type,
    be.click_id AS click_id,
    de.user_custom_id AS user_custom_id,
    be.browser_name AS browser_name,
    de.os_name AS os_name,
    de.device_type AS device_type,
    ge.geo_country AS geo_country,
    ge.geo_region_name AS geo_region_name,
    le.page_url AS page_url,
    le.referer_url AS referer_url,
    le.utm_source AS utm_source,
    le.utm_medium AS utm_medium,
    le.utm_campaign AS utm_campaign
FROM
    browser_events AS be
LEFT JOIN device_events AS de ON be.click_id = de.click_id
LEFT JOIN geo_events AS ge ON be.click_id = ge.click_id
LEFT JOIN location_events AS le ON be.event_id = le.event_id;
```

---

### 4.2. Создание вьюхи `purchase_sessions`


```sql
CREATE VIEW purchase_sessions AS
SELECT
    click_id,
    payment_time,
    confirmation_time
FROM (
    SELECT
        click_id,
        MIN(CASE WHEN page_url LIKE '%/payment%' THEN event_time END) AS payment_time,
        MIN(CASE WHEN page_url LIKE '%/confirmation%' THEN event_time END) AS confirmation_time
    FROM mv_union_events
    WHERE page_url LIKE '%/payment%' OR page_url LIKE '%/confirmation%'
    GROUP BY click_id
    HAVING
        payment_time IS NOT NULL
        AND confirmation_time IS NOT NULL
        AND payment_time < confirmation_time
) AS t;
```
---

## 5. Дашборд

Используем подготовленные данные для анализа с помощью Datalens: 
[-CLICK-](https://datalens.yandex/h1g14q77080w4)
