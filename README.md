# Въведение

Това е PoC проект за конвертиране на граф образователните данни в
релационна база.

В първият етап от прокета
* конвертират се само част от данните - населени места, училища,
ДЗИ , резултати от ДЗИ.
* ползва се sqlite база


Тъй като се ползва sqlalchemy библиотека, то теоритично в комбинация с
ODBC драйвер би трябвало данните да могат лесно да отидат и в BigQuery.

# Работа с python venv

requirements.txt съдържа нужните библиотеки, с негова помощ може да се
създаде python venv.


# Работа с ДБ миграции

Използва се библиотека `alembic`. На този етап всичко се случва ръчно.


Празна sqlite база се създава така:
```
sqlite3 /path/to/file.sqlite "VACUUM;"
```

Конфигуриране на `aliembic` става във файл `src/db_migrations/env.py`,
като се редактира следният ред:
```
sqlalchemy.url = sqlite:///<absoute path to the sqlite db, there should be four slashes!>
```

Изпълнение на съществуващите ДБ миграции:
```
cd src/
<activate your python env>
alembic upgrade head
```

Генериране на нови ДБ миграции (след промяна в `src/main/models.py`)
```
cd src/
<activate your python env>
alembic revision --autogenerate -m '<descriptin of the models change>'
alembic upgrade head
```