
SET SQL DIALECT 3; 

/* CREATE DATABASE 'localhost:gstat' PAGE_SIZE 16384 DEFAULT CHARACTER SET UTF8; */


/*  Generators or sequences */
CREATE GENERATOR DB_ID;
CREATE GENERATOR IDX_ID;
CREATE GENERATOR TBL_ID;

/* Domain definitions */
CREATE DOMAIN D_BIGINT AS BIGINT;
CREATE DOMAIN D_DATE AS DATE;
CREATE DOMAIN D_DATETIME AS TIMESTAMP;
CREATE DOMAIN D_FLOAT AS FLOAT;
CREATE DOMAIN D_ID AS BIGINT;
CREATE DOMAIN D_FILE_NAME AS VARCHAR(255);		/*CREATE DOMAIN D_VARCHAR255 AS VARCHAR(255); */
CREATE DOMAIN D_TABLE_NAME AS CHAR(31);
CREATE DOMAIN D_INDEX_NAME AS CHAR(31);			/*CREATE DOMAIN D_VARCHAR31 AS VARCHAR(31);*/
CREATE DOMAIN D_ODS_VERSION AS VARCHAR(16);
CREATE DOMAIN D_VARCHAR64 AS VARCHAR(64);
COMMIT WORK;

/* Table: DB, Owner: SYSDBA */
CREATE TABLE DB (ID D_BIGINT NOT NULL,
        "DATE" D_DATE NOT NULL,
        NAME D_FILE_NAME NOT NULL,
        "PAGE_SIZE" D_BIGINT NOT NULL,
        CREATE_DATE D_DATETIME,
        PAGES_TOTAL D_BIGINT,
        GENERATION D_BIGINT,
        ODS_VERSION D_ODS_VERSION,
        OLDEST_TRANSACTION D_BIGINT,
        OLDEST_ACTIVE D_BIGINT,
        OLDEST_SNAPSHOT D_BIGINT,
        NEXT_TRANSACTION D_BIGINT,
        BUMPED_TRANSACTION D_BIGINT,
        SEQUENCE_NUMBER D_BIGINT,
        NEXT_ATTACHMENT_ID D_BIGINT,
        IMPLEMENTATION D_VARCHAR64,
        SHADOW_COUNT D_BIGINT,
        PAGE_BUFFERS D_BIGINT,
        NEXT_HEADER_PAGE D_BIGINT,
        DATABASE_DIALECT D_BIGINT,
        ATTRIBUTES D_VARCHAR64,
CONSTRAINT PK_DB PRIMARY KEY (ID),
CONSTRAINT UNQ1_DB UNIQUE ("DATE", NAME));

/* Table: IDX, Owner: SYSDBA */
CREATE TABLE IDX (DB_ID D_ID NOT NULL,
        TBL_ID D_ID NOT NULL,
        ID D_ID NOT NULL,
        NAME D_INDEX_NAME NOT NULL,
        DEPTH D_BIGINT,
        LEAF_BUCKETS D_BIGINT,
        NODES D_BIGINT,
        AVG_LENGTH D_FLOAT,
        DUP_TOTAL D_BIGINT,
        DUP_MAX D_BIGINT,
        FILL_20 D_BIGINT,
        FILL_40 D_BIGINT,
        FILL_60 D_BIGINT,
        FILL_80 D_BIGINT,
        FILL_99 D_BIGINT,
        PAGES_TOTAL BIGINT COMPUTED BY (NULL),
        AVG_NODE_LEN D_FLOAT,
        AVG_KEY_LEN D_FLOAT,
        COMPRESSION_RATIO D_FLOAT,
        AVG_PREFIX_LEN D_FLOAT,
        CLUSTER_FACTOR D_BIGINT,
        CLUSTER_RATIO D_FLOAT,
CONSTRAINT PK_IDX PRIMARY KEY (ID));

/* Table: TBL, Owner: SYSDBA */
CREATE TABLE TBL (DB_ID D_ID NOT NULL,
        ID D_ID NOT NULL,
        NAME D_TABLE_NAME NOT NULL,
        REC_AVG_LEN D_FLOAT,
        REC_TOTAL D_BIGINT,
        VER_AVG_LEN D_FLOAT,
        VER_TOTAL D_BIGINT,
        VER_MAX D_BIGINT,
        FRAG_AVG_LEN D_FLOAT,
        FRAG_TOTAL D_BIGINT,
        FRAG_MAX D_BIGINT,
        BLOB_TOTAL D_BIGINT,
        BLOB_TOTAL_LENGTH D_BIGINT,
        BLOB_PAGES D_BIGINT,
        BLOB_LEVEL1 D_BIGINT,
        BLOB_LEVEL2 D_BIGINT,
        BLOB_LEVEL3 D_BIGINT,
        PAGES_DATA D_BIGINT,
        PAGES_SLOT D_BIGINT,
        PAGES_FILL_AVG D_BIGINT,
        PAGES_BIG D_BIGINT,
        FILL_20 D_BIGINT,
        FILL_40 D_BIGINT,
        FILL_60 D_BIGINT,
        FILL_80 D_BIGINT,
        FILL_99 D_BIGINT,
        FORMATS_TOTAL D_BIGINT,
        FORMATS_USED D_BIGINT,
        AVG_UNPACK_LEN D_FLOAT,
        COMPRESS_RATIO D_FLOAT,
        POINTER_PAGES D_BIGINT,
        PRIMARY_PAGES D_BIGINT,
        SECONDARY_PAGES D_BIGINT,
        SWEPT_PAGES D_BIGINT,
        EMPTY_PAGES D_BIGINT,
        FULL_PAGES D_BIGINT,
CONSTRAINT PK_TBL PRIMARY KEY (ID));

COMMIT WORK;
SET AUTODDL OFF;
SET TERM ^ ;

/* Stored procedures headers */
CREATE OR ALTER PROCEDURE BLOB_SIZE_TBL (DB_LIKE D_FILE_NAME NOT NULL,
DB_DATE D_DATE NOT NULL)
RETURNS (TBL_NAME D_TABLE_NAME,
TOTAL_SIZE D_BIGINT,
DATA_SIZE D_BIGINT,
BLOB_SIZE D_BIGINT)
AS 
BEGIN SUSPEND; END ^
CREATE OR ALTER PROCEDURE DB_SIZE_TBL (DB_NAME_STW D_FILE_NAME NOT NULL,
DB_DATE D_DATE NOT NULL)
RETURNS (DB_NAME D_FILE_NAME,
TBL_NAME D_TABLE_NAME,
TOTAL_SIZE D_BIGINT,
DATA_SIZE D_BIGINT,
IDX_SIZE D_BIGINT,
BLOB_SIZE D_BIGINT)
AS 
BEGIN SUSPEND; END ^
CREATE OR ALTER PROCEDURE TBL_SIZE_RBD (DB_LIKE D_FILE_NAME,
TBL_NAME D_TABLE_NAME NOT NULL,
DB_DATE D_DATE NOT NULL)
RETURNS (DB_NAME D_FILE_NAME,
TOTAL_SIZE D_BIGINT,
DATA_SIZE D_BIGINT,
IDX_SIZE D_BIGINT,
BLOB_SIZE D_BIGINT)
AS 
BEGIN SUSPEND; END ^

SET TERM ; ^
COMMIT WORK;
SET AUTODDL ON;

/*  Index definitions for all user tables */
CREATE INDEX IDX_IDX1 ON IDX (NAME);
CREATE INDEX TBL_IDX1 ON TBL (NAME);

ALTER TABLE IDX ADD CONSTRAINT FK_IDX_1 FOREIGN KEY (TBL_ID) REFERENCES TBL (ID);

ALTER TABLE IDX ADD CONSTRAINT FK_IDX_2 FOREIGN KEY (DB_ID) REFERENCES DB (ID);

ALTER TABLE TBL ADD CONSTRAINT FK_TBL_DB FOREIGN KEY (DB_ID) REFERENCES DB (ID);

COMMIT WORK;
SET AUTODDL OFF;
SET TERM ^ ;

/* Stored procedures bodies */

ALTER PROCEDURE BLOB_SIZE_TBL (DB_LIKE D_FILE_NAME NOT NULL,
DB_DATE D_DATE NOT NULL)
RETURNS (TBL_NAME D_TABLE_NAME,
TOTAL_SIZE D_BIGINT,
DATA_SIZE D_BIGINT,
BLOB_SIZE D_BIGINT)
AS 
declare variable DB_ID bigint;
begin
--  select id from DB where DB."NAME" like :DB_LIKE and DB."DATE"=:DB_DATE into :DB_ID;
  for select TBL.name,
    sum((COALESCE(TBL.pages_data, 0)*DB."PAGE_SIZE")/1024/1024) as PAGES_SIZE_MB,
    sum((COALESCE(TBL.blob_pages, 0)*DB."PAGE_SIZE")/1024/1024) as BLOB_SIZE_MB
  from DB join TBL on DB.ID=TBL.DB_ID
    where DB."NAME" like :DB_LIKE
    group by 1
  into :tbl_name, :DATA_SIZE, :blob_size
  do
  begin
    select :DATA_SIZE + :blob_size from rdb$database into :total_size;
    suspend;
  end
end ^

ALTER PROCEDURE DB_SIZE_TBL (DB_NAME_STW D_FILE_NAME NOT NULL,
DB_DATE D_DATE NOT NULL)
RETURNS (DB_NAME D_FILE_NAME,
TBL_NAME D_TABLE_NAME,
TOTAL_SIZE D_BIGINT,
DATA_SIZE D_BIGINT,
IDX_SIZE D_BIGINT,
BLOB_SIZE D_BIGINT)
AS 
declare variable DB_ID bigint;
begin
  for select id, substring("NAME" from 1 for 31)
  from DB
  where DB."NAME" starting with :DB_NAME_STW and DB."DATE"=:DB_DATE
  into :DB_ID, :DB_NAME
  do
  begin
    for select TBL.name,
      (COALESCE(TBL.pages_data, 0)*DB."PAGE_SIZE")/1024/1024 as PAGES_SIZE_MB,
      (COALESCE(TBL.blob_pages, 0)*DB."PAGE_SIZE")/1024/1024 as BLOB_SIZE_MB,
      sum(IDX.PAGES_TOTAL*DB."PAGE_SIZE"/1024/1024) as IDX_SIZE_MB
    from DB join TBL on DB.ID=TBL.DB_ID
      left join IDX on TBL.id=IDX.tbl_id
      where DB.ID=:DB_ID
      group by 1, 2, 3
    into :tbl_name, :DATA_SIZE, :blob_size, :idx_size
    do
    begin
      select :DATA_SIZE + :blob_size + :idx_size from rdb$database into :total_size;
      suspend;
    end
  end
end ^

ALTER PROCEDURE TBL_SIZE_RBD (DB_LIKE D_FILE_NAME,
TBL_NAME D_TABLE_NAME NOT NULL,
DB_DATE D_DATE NOT NULL)
RETURNS (DB_NAME D_TABLE_NAME,
TOTAL_SIZE D_BIGINT,
DATA_SIZE D_BIGINT,
IDX_SIZE D_BIGINT,
BLOB_SIZE D_BIGINT)
AS 
declare variable DB_PAGE_SIZE bigint;
declare variable DB_ID bigint;
begin
  for select DB.name, DB.id, DB."PAGE_SIZE"
  from db
  where db."DATE"=:db_date and db."NAME" like :db_like
  into :db_name, :DB_ID, :DB_PAGE_SIZE do
  begin
    for select
        (COALESCE(TBL.pages_data, 0)*:DB_PAGE_SIZE)/1024/1024 as PAGES_SIZE_MB,
        (COALESCE(TBL.blob_pages, 0)*:DB_PAGE_SIZE)/1024/1024 as BLOB_SIZE_MB,
        sum(IDX.PAGES_TOTAL)*:DB_PAGE_SIZE/1024/1024 as IDX_SIZE_MB
    from TBL join IDX on TBL.id=IDX.tbl_id
    where TBL.DB_ID=:DB_ID and TBL."NAME"=:tbl_name
    group by 1, 2
    into :DATA_SIZE, :BLOB_SIZE, :IDX_SIZE do
    begin
      select :DATA_SIZE + :IDX_SIZE + :BLOB_SIZE from rdb$database into :total_size;
      suspend;
    end
  end
end ^

SET TERM ; ^
COMMIT WORK;
SET AUTODDL ON;

/* Computed fields */

ALTER TABLE IDX 
        ALTER PAGES_TOTAL TYPE BIGINT COMPUTED BY (FILL_20+FILL_40+FILL_60+FILL_80+FILL_99);

SET TERM ^ ;

/* Triggers only will work for SQL triggers */
CREATE TRIGGER DB_BI0 FOR DB 
ACTIVE BEFORE INSERT POSITION 0 
AS
begin
  if (new.id is null) then
    new.id = gen_id(db_id, 1);
end ^

CREATE TRIGGER DB_BD0 FOR DB 
ACTIVE BEFORE DELETE POSITION 0 
AS
begin
  delete from idx where db_id=old.id;
  delete from tbl where db_id=old.id;
end ^

CREATE TRIGGER IDX_BI0 FOR IDX 
ACTIVE BEFORE INSERT POSITION 0 
AS
begin
  if (new.id is null) then
    new.id = gen_id(idx_id, 1);
end ^

CREATE TRIGGER TBL_BI0 FOR TBL 
ACTIVE BEFORE INSERT POSITION 0 
AS
begin
  if (new.id is null) then
    new.id = gen_id(tbl_id, 1);
end ^


SET TERM ; ^
COMMIT WORK;
