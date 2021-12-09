# About the project
Python script to import text output of gstat utility (Firebird RDBMS) into DB.
[sql/meta.sql](sql/meta.sql) - file contains the database structure.

# Requirements
fdb - Python driver for Firebird RDBMS

# Usage example

## Import 
Import from command line:

    python gstat2sql.py --user SYSDBA --passwd masterkey --dsn localhost:gstat employee.gstat

Import from python script:

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

## Analyse

Статистика по размеру таблиц в БД. Размер в разрезе данные, индексы, BLOB:

    select tbl_name, total_size, data_size, idx_size, blob_size
    from DB_SIZE_TBL('/var/lib/firebird/employee.fdb', '2021-12-01')
    order by total_size desc

Статистика по размеру таблиц в нескольких БД имя которых начинается с rbd-. Размер в разрезе данные, индексы, BLOB:

    select tbl_name, sum(total_size), sum(data_size), sum(idx_size), sum(blob_size)
    from DB_SIZE_TBL('rbd-', '2021-12-01')
    group by tbl_name
    order by sum(total_size) desc

# Restrictions
Tested on Firebird 2, Firebird 3
