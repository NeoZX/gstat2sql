[EN](README.md) [RU](README.ru.md)

# About the project
Python script to import text output of gstat utility (Firebird RDBMS) into DB.
[sql/meta.sql](sql/meta.sql) - file contains the database structure.

# Requirements
* Python 3.9
* fdb - Python driver for Firebird RDBMS

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

Statistics on the size of tables in the database. Size in section data, indexes, BLOB:

    select tbl_name, total_size, data_size, idx_size, blob_size
    from DB_SIZE_TBL('/var/lib/firebird/employee.fdb', '2021-12-01')
    order by total_size desc

Statistics on the size of tables in several databases whose name like[^1] 'rbd-'. Size in section data, indexes, BLOB:

    select tbl_name, sum(total_size), sum(data_size), sum(idx_size), sum(blob_size)
    from DB_SIZE_TBL('rbd-', '2021-12-01')
    group by tbl_name
    order by sum(total_size) desc

Size of tables in databases for several periods:

    select P19.TBL_NAME, P19.TOTAL_SIZE as P19, P20.TOTAL_SIZE as P20, P21.TOTAL_SIZE as P21
    from DB_SIZE_TBL(:DB_NAME, '2019-12-01') as P19
        join DB_SIZE_TBL(:DB_NAME, '2020-12-01') as P20 on P19.TBL_NAME=P20.TBL_NAME
        join DB_SIZE_TBL(:DB_NAME, '2021-12-01') as P21 on P19.TBL_NAME=P21.TBL_NAME
    order by P19.TOTAL_SIZE desc

BLOB size by tables in databases whose name is like 'rbd-':

    select *
    from BLOB_SIZE_TBL(:DB_LIKE, '2021-12-01')
    order by BLOB_SIZE desc

# Restrictions
Tested on Firebird 2/3/4/5, RedDatabase 2.5/2.6/3/5

[^1]: Here and below, like means an sql-operator.
