# coding=UTF-8

import re
import fdb
from datetime import date


class DBHeaderStat:
    """Class description"""
    def __init__(self):
        """Class init description here"""
        self.value = {
            'database': None,
            'flags': None,
            'generation': None,
            'sys_change_num': None,
            'page_size': None,
            'server': None,
            'ods_version': None,
            'oldest_transaction': None,
            'oldest_active': None,
            'oldest_snapshot': None,
            'next_transaction': None,
            'bumped_transaction': None,
            'sequence_number': None,
            'next_attachment_id': None,
            'implementation': None,
            'shadow_count': None,
            'page_buffers': None,
            'next_header_page': None,
            'database_dialect': None,
            'create_date': None,
            'attributes': None
        }
        self.re_skip_lines = re.compile('^(Gstat execution|'
                                        'Database header page information|'
                                        '\tFlags\t\t\t|'
                                        '\tChecksum\t\t|'
                                        '\tImplementation ID\t).+$')
        self.re = {
            'database': re.compile('^Database "(.+)"$'),
            'flags': re.compile('^\tFlags\t\t([0-9]+)$'),
            'generation': re.compile('^\tGeneration\t\t([0-9]+)$'),
            'sys_change_num': re.compile('^\tSystem Change Number\t([0-9]+)$'),
            'page_size': re.compile('^\tPage size\t\t([0-9]+)$'),
            'server': re.compile('^\tServer\t\t\t(.+)$'),
            'ods_version': re.compile('^\tODS version\t\t([0-9\.]+)$'),
            'oldest_transaction': re.compile('^\tOldest transaction\t([0-9]+)$'),
            'oldest_active': re.compile('^\tOldest active\t\t([0-9]+)$'),
            'oldest_snapshot': re.compile('^\tOldest snapshot\t\t([0-9]+)$'),
            'next_transaction': re.compile('^\tNext transaction\t([0-9]+)$'),
            'bumped_transaction': re.compile('\tBumped transaction\t([0-9]+)$'),
            'sequence_number': re.compile('^\tSequence number\t\t([0-9]+)$'),
            'next_attachment_id': re.compile('\tNext attachment ID\t([0-9]+)$'),
            'implementation': re.compile('\tImplementation\t\t(.+)$'),
            'shadow_count': re.compile('\tShadow count\t\t([0-9]+)$'),
            'page_buffers': re.compile('\tPage buffers\t\t([0-9]+)$'),
            'next_header_page': re.compile('\tNext header page\t([0-9]+)$'),
            'database_dialect': re.compile('\tDatabase dialect\t([0-9]+)$'),
            'create_date': re.compile('\tCreation date\t\t([A-Za-z]{3} [0-9]{1,2}, [0-9]{4} [0-9:]{7,8})$'),
            'attributes': re.compile('\tAttributes\t\t(.+)$')
        }

    def process_line(self, line):
        for key in self.re.keys():
            if self.re[key].search(line):
                self.value[key] = self.re[key].search(line).group(1)
                return 'ok'
        return None


class TableStat:
    """Class description"""
    def __init__(self):
        """Class init description here"""
        self.name = None
        self.rec_avg_len = None
        self.rec_total = None
        self.ver_avg_len = None
        self.ver_total = None
        self.ver_max = None
        self.frag_avg_len = None
        self.frag_total = None
        self.frag_max = None
        self.blob_total = None
        self.blob_total_length = None
        self.blob_pages = None
        self.blob_level0 = None
        self.blob_level1 = None
        self.blob_level2 = None
        self.pages_data = None
        self.pages_slot = None
        self.pages_fill_avg = None
        self.pages_big = None
        self.fill_20 = None
        self.fill_40 = None
        self.fill_60 = None
        self.fill_80 = None
        self.fill_99 = None
        self.re = {
            'name': re.compile('^([0-9A-Z_$]+) \([0-9]+\)$'),
            'ppp': re.compile('^ {4}Primary pointer page: ([0-9]+), Index root page: ([0-9]+)$'),
            'avg_rec_len': re.compile('^ {4}Average record length: ([0-9\.]+), total records: ([0-9]+)'),
            'avg_ver_len': re.compile(
                '^ {4}Average version length: ([0-9.]+), total versions: ([0-9]+), max versions: ([0-9]+)$'),
            'avg_fr_len': re.compile(
                '^ {4}Average fragment length: ([0-9.]+), total fragments: ([0-9]+), max fragments: ([0-9]+)$'),
            'blobs': re.compile('^ {4}Blobs: ([0-9]+), total length: ([0-9]+), blob pages: ([0-9]+)$'),
            'blobs_levels': re.compile('^ {8}Level 0: ([0-9]+), Level 1: ([0-9]+), Level 2: ([0-9]+)$'),
            'data_pages': re.compile('^ {4}Data pages: ([0-9]+), data page slots: ([0-9]+), average fill: ([0-9]+)%'),
            'big_record_pages': re.compile('^ {4}Big record pages: ([0-9]+)$'),
            'fill_distribution': re.compile('^ {4}Fill distribution:$'),
            'fill_0-19': re.compile('^\t 0 - 19% = ([0-9]+)$'),
            'fill_20-39': re.compile('^\t20 - 39% = ([0-9]+)$'),
            'fill_40-59': re.compile('^\t40 - 59% = ([0-9]+)$'),
            'fill_60-79': re.compile('^\t60 - 79% = ([0-9]+)$'),
            'fill_80-99': re.compile('^\t80 - 99% = ([0-9]+)$')
        }

    def reset_stat(self):
        self.name = None
        self.rec_avg_len = None
        self.rec_total = None
        self.ver_avg_len = None
        self.ver_total = None
        self.ver_max = None
        self.frag_avg_len = None
        self.frag_total = None
        self.frag_max = None
        self.blob_total = None
        self.blob_total_length = None
        self.blob_pages = None
        self.blob_level0 = None
        self.blob_level1 = None
        self.blob_level2 = None
        self.pages_data = None
        self.pages_slot = None
        self.pages_fill_avg = None
        self.pages_big = None
        self.fill_20 = None
        self.fill_40 = None
        self.fill_60 = None
        self.fill_80 = None
        self.fill_99 = None


class IndexStat:
    """Class description here"""
    def __init__(self):
        """Class init"""
        self.name = None
        self.depth = None
        self.leaf_buckets = None
        self.nodes = None
        self.avg_length = None
        self.dup_total = None
        self.dup_max = None
        self.fill_20 = None
        self.fill_40 = None
        self.fill_60 = None
        self.fill_80 = None
        self.fill_99 = None
        self.re = {
            'name': re.compile('^ {4}Index ([0-9A-Z_$]+) \(([0-9]+)\)$'),
            'depth': re.compile('^\tDepth: ([0-9]+), leaf buckets: ([0-9]+), nodes: ([0-9]+)$'),
            'avg_data_len': re.compile('^\tAverage data length: ([0-9.]+), total dup: ([0-9]+), max dup: ([0-9]+)$'),
            'fill_distribution': re.compile('^\tFill distribution:$'),
            'fill_0-19': re.compile('^\t {4} 0 - 19% = ([0-9]+)$'),
            'fill_20-39': re.compile('^\t {4}20 - 39% = ([0-9]+)$'),
            'fill_40-59': re.compile('^\t {4}40 - 59% = ([0-9]+)$'),
            'fill_60-79': re.compile('^\t {4}60 - 79% = ([0-9]+)$'),
            'fill_80-99': re.compile('^\t {4}80 - 99% = ([0-9]+)$')
        }

    def reset_stat(self):
        self.name = None
        self.depth = None
        self.leaf_buckets = None
        self.nodes = None
        self.avg_length = None
        self.dup_total = None
        self.dup_max = None
        self.fill_20 = None
        self.fill_40 = None
        self.fill_60 = None
        self.fill_80 = None
        self.fill_99 = None


class GStatToSQL:
    """Class description here"""

    def __init__(self, gstat_file, gstat_date=date.today(), dsn='', db_user=None, db_pass=None, db_charset='UTF-8'):
        """Class init description here"""
        self.gstat_file = gstat_file
        self.gstat_date = gstat_date
        self.dsn = dsn
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_charset = db_charset
        self.db_name = None
        self.page_size = None
        self.create_date = None

    def processing(self):
        """Class functions descriptions"""
        fd = open(self.gstat_file)
        line = 'start'
        db_header = DBHeaderStat()
        while line != '':
            line = fd.readline()
            if line == "Analyzing database pages ...\n":
                break
            if line == "\n":
                pass
            elif db_header.re_skip_lines.search(line):
                pass
            elif db_header.process_line(line):
                pass
            elif line == "    Variable header data:\n":
                #Skip Variable header data
                while line != "Analyzing database pages ...\n":
                    line = fd.readline()
                break
            else:
                print("Unrecognized string at ", fd.tell())
                print(line)
                exit(1)
        connection = fdb.connect(dsn=self.dsn, user=self.db_user, password=self.db_pass, charset=self.db_charset)
        # begin transaction
        cursor = connection.cursor()
        # insert into DB returning id
        cursor.execute("update or insert into DB (\"NAME\", PAGE_SIZE, CREATE_DATE, "
                       "\"DATE\", GENERATION, ODS_VERSION, "
                       "OLDEST_TRANSACTION, OLDEST_ACTIVE, "
                       "OLDEST_SNAPSHOT, NEXT_TRANSACTION, "
                       "BUMPED_TRANSACTION, SEQUENCE_NUMBER, "
                       "NEXT_ATTACHMENT_ID, IMPLEMENTATION, "
                       "SHADOW_COUNT, PAGE_BUFFERS, "
                       "NEXT_HEADER_PAGE, DATABASE_DIALECT, "
                       "ATTRIBUTES) "
                       "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                       "matching (\"NAME\", \"DATE\") returning ID;",
                       (db_header.value['database'], db_header.value['page_size'], db_header.value['create_date'],
                        self.gstat_date, db_header.value['generation'], db_header.value['ods_version'],
                        db_header.value['oldest_transaction'], db_header.value['oldest_active'],
                        db_header.value['oldest_snapshot'], db_header.value['next_transaction'],
                        db_header.value['bumped_transaction'], db_header.value['sequence_number'],
                        db_header.value['next_attachment_id'], db_header.value['implementation'],
                        db_header.value['shadow_count'], db_header.value['page_buffers'],
                        db_header.value['next_header_page'], db_header.value['database_dialect'],
                        db_header.value['attributes']))
        db_id = cursor.fetchone()[0]
        table = TableStat()
        tbl_id = None
        index = IndexStat()
        line = fd.readline()
        while line != '':
            # Processing table
            if table.re['name'].search(line):
                table.name = table.re['name'].search(line).group(1)
                line = fd.readline()
                if table.re['ppp'].search(line):
                    line = fd.readline()
                continue
            elif table.re['avg_rec_len'].search(line):
                table.rec_avg_len = table.re['avg_rec_len'].search(line).group(1)
                table.rec_total = table.re['avg_rec_len'].search(line).group(2)
                line = fd.readline()
                if table.re['avg_ver_len'].search(line):
                    table.ver_avg_len = table.re['avg_ver_len'].search(line).group(1)
                    table.ver_total = table.re['avg_ver_len'].search(line).group(2)
                    table.ver_max = table.re['avg_ver_len'].search(line).group(3)
                    line = fd.readline()
                    if table.re['avg_fr_len'].search(line):
                        table.frag_avg_len = table.re['avg_fr_len'].search(line).group(1)
                        table.frag_total = table.re['avg_fr_len'].search(line).group(2)
                        table.frag_max = table.re['avg_fr_len'].search(line).group(3)
                        line = fd.readline()
                continue
            elif table.re['blobs'].search(line):
                table.blob_total = table.re['blobs'].search(line).group(1)
                table.blob_total_length = table.re['blobs'].search(line).group(2)
                table.blob_pages = table.re['blobs'].search(line).group(3)
                line = fd.readline()
                if table.re['blobs_levels'].search(line):
                    table.blob_level0 = table.re['blobs_levels'].search(line).group(1)
                    table.blob_level1 = table.re['blobs_levels'].search(line).group(2)
                    table.blob_level2 = table.re['blobs_levels'].search(line).group(3)
                    line = fd.readline()
                continue
            elif table.re['data_pages'].search(line):
                table.pages_data = table.re['data_pages'].search(line).group(1)
                table.pages_slot = table.re['data_pages'].search(line).group(2)
                table.pages_fill_avg = table.re['data_pages'].search(line).group(3)
            elif table.re['big_record_pages'].search(line):
                table.pages_big = table.re['big_record_pages'].search(line).group(1)
            elif table.re['fill_distribution'].search(line):
                line = fd.readline()
                if table.re['fill_0-19'].search(line):
                    table.fill_20 = table.re['fill_0-19'].search(line).group(1)
                    line = fd.readline()
                    if table.re['fill_20-39'].search(line):
                        table.fill_40 = table.re['fill_20-39'].search(line).group(1)
                        line = fd.readline()
                        if table.re['fill_40-59'].search(line):
                            table.fill_60 = table.re['fill_40-59'].search(line).group(1)
                            line = fd.readline()
                            if table.re['fill_60-79'].search(line):
                                table.fill_70 = table.re['fill_60-79'].search(line).group(1)
                                line = fd.readline()
                                if table.re['fill_80-99'].search(line):
                                    table.fill_99 = table.re['fill_80-99'].search(line).group(1)
                                    line = fd.readline()
                continue
            # Processing index
            elif index.re['name'].search(line):
                index.name = index.re['name'].search(line).group(1)
                line = fd.readline()
                if index.re['depth'].search(line):
                    index.depth = index.re['depth'].search(line).group(1)
                    index.leaf_buckets = index.re['depth'].search(line).group(2)
                    index.nodes = index.re['depth'].search(line).group(3)
                    line = fd.readline()
                    if index.re['avg_data_len'].search(line):
                        index.avg_length = index.re['avg_data_len'].search(line).group(1)
                        index.dup_total = index.re['avg_data_len'].search(line).group(2)
                        index.dup_max = index.re['avg_data_len'].search(line).group(3)
                        line = fd.readline()
                        if index.re['fill_distribution'].search(line):
                            line = fd.readline()
                            if index.re['fill_0-19'].search(line):
                                index.fill_20 = index.re['fill_0-19'].search(line).group(1)
                                line = fd.readline()
                                if index.re['fill_20-39'].search(line):
                                    index.fill_40 = index.re['fill_20-39'].search(line).group(1)
                                    line = fd.readline()
                                    if index.re['fill_40-59'].search(line):
                                        index.fill_60 = index.re['fill_40-59'].search(line).group(1)
                                        line = fd.readline()
                                        if index.re['fill_60-79'].search(line):
                                            index.fill_80 = index.re['fill_60-79'].search(line).group(1)
                                            line = fd.readline()
                                            if index.re['fill_80-99'].search(line):
                                                index.fill_99 = index.re['fill_80-99'].search(line).group(1)
                                                line = fd.readline()
                continue
            elif line == '\n':
                if table.name:
                    cursor.execute(
                        "update or insert into TBL (DB_ID, \"NAME\", REC_AVG_LEN, REC_TOTAL, "
                        "VER_AVG_LEN, VER_TOTAL, VER_MAX, "
                        "FRAG_AVG_LEN, FRAG_TOTAL, FRAG_MAX, "
                        "BLOB_TOTAL, BLOB_TOTAL_LENGTH, BLOB_PAGES, "
                        "BLOB_LEVEL1, BLOB_LEVEL2, BLOB_LEVEL3, "
                        "PAGES_DATA, PAGES_SLOT, PAGES_FILL_AVG, PAGES_BIG, "
                        "FILL_20, FILL_40, FILL_60, FILL_80, FILL_99) "
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        "matching (DB_ID, \"NAME\") "
                        "returning ID;",
                        (db_id, table.name, table.rec_avg_len, table.rec_total,
                         table.ver_avg_len, table.ver_total, table.ver_max,
                         table.frag_avg_len, table.frag_total, table.frag_max,
                         table.blob_total, table.blob_total_length, table.blob_pages,
                         table.blob_level0, table.blob_level1, table.blob_level2,
                         table.pages_data, table.pages_slot, table.pages_fill_avg, table.pages_big,
                         table.fill_20, table.fill_40, table.fill_60, table.fill_80, table.fill_99))
                    tbl_id = cursor.fetchone()[0]
                    table.reset_stat()
                elif index.name:
                    # insert data about index
                    cursor.execute(
                        "update or insert into IDX (DB_ID, TBL_ID, \"NAME\", DEPTH, LEAF_BUCKETS, NODES, "
                        "AVG_LENGTH, DUP_TOTAL, DUP_MAX, "
                        "FILL_20, FILL_40, FILL_60, FILL_80, FILL_99) "
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        "matching (DB_ID, TBL_ID, \"NAME\");",
                        (db_id, tbl_id, index.name, index.depth, index.leaf_buckets, index.nodes,
                         index.avg_length, index.dup_total, index.dup_max,
                         index.fill_20, index.fill_40, index.fill_60, index.fill_80, index.fill_99)
                    )
                    index.reset_stat()
            else:
                print("Unrecognized string at ", fd.tell())
                print(line)
                connection.rollback()
                exit(1)
            line = fd.readline()
        connection.commit()
        fd.close()
