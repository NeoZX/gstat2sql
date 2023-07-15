[EN](README.md) [RU](README.ru.md)

# О проекте
Python script to import text output of gstat utility (Firebird RDBMS) into DB.
[sql/meta.sql](sql/meta.sql) - file contains the database structure.

# Требования
* Python 3.9
* fdb - Питон драйвер для доступа к СУБД Firebird

# Примеры использования

## Импорт
Импорт из командной строки:

    python gstat2sql.py --user SYSDBA --passwd masterkey --dsn localhost:gstat employee.gstat

Импорт из Python программы:

    import gstat2sql
    import os
    directory = '/directory/with/gstat/files/'
    for filename in sorted(os.listdir(directory)):
        if os.path.isfile(directory + filename):
            print("Loading " + filename)
            gstat = gstat2sql.GStatToSQL(gstat_file=directory + filename,
                                         gstat_date='2021-12-01', 
                                         dsn='localhost:gstat', 
                                         db_user='SYSDBA',
                                         db_pass='masterkey')
            gstat.processing()

## Анализ

Статистика по размеру таблиц в БД. Размер в разрезе данные, индексы, BLOB:

    select tbl_name, total_size, data_size, idx_size, blob_size
    from DB_SIZE_TBL('/var/lib/firebird/employee.fdb', '2021-12-01')
    order by total_size desc

Статистика по размеру таблиц в нескольких БД, имя которых like[^1] 'rbd-'. Размер в разрезе данные, индексы, BLOB:

    select tbl_name, sum(total_size), sum(data_size), sum(idx_size), sum(blob_size)
    from DB_SIZE_TBL('rbd-', '2021-12-01')
    group by tbl_name
    order by sum(total_size) desc

Размер таблиц в базах по нескольким периодам:

    select P19.TBL_NAME, P19.TOTAL_SIZE as P19, P20.TOTAL_SIZE as P20, P21.TOTAL_SIZE as P21
    from DB_SIZE_TBL(:DB_NAME, '2019-12-01') as P19
        join DB_SIZE_TBL(:DB_NAME, '2020-12-01') as P20 on P19.TBL_NAME=P20.TBL_NAME
        join DB_SIZE_TBL(:DB_NAME, '2021-12-01') as P21 on P19.TBL_NAME=P21.TBL_NAME
    order by P19.TOTAL_SIZE desc

Размер BLOB по таблицам в базах, имя которых like 'rbd-':

    select *
    from BLOB_SIZE_TBL(:DB_LIKE, '2021-12-01')
    order by BLOB_SIZE desc

# Ограничения
Протестировано на Firebird 2/3/4/5, Ред Базе Данных 2.5/2.6/3/5

[^1]: Здесь и далее под like подразумевается sql-оператор.
