# coding=UTF-8

import re
import fdb
from datetime import date, datetime


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
                                        '\tImplementation ID\t|'
                                        '\tAutosweep gap\t\t).+$')
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
        self.tabel_space = None
        self.formats_total = None
        self.formats_used = None
        self.rec_avg_len = None
        self.rec_total = None
        self.ver_avg_len = None
        self.ver_total = None
        self.ver_max = None
        self.frag_avg_len = None
        self.frag_total = None
        self.frag_max = None
        self.avg_unpack_length = None
        self.compress_ratio = None
        self.blob_total = None
        self.blob_total_length = None
        self.blob_pages = None
        self.blob_level0 = None
        self.blob_level1 = None
        self.blob_level2 = None
        self.pointer_pages = None
        self.pages_data = None
        self.pages_slot = None
        self.pages_fill_avg = None
        self.primary_pages = None
        self.secondary_pages = None
        self.swept_pages = None
        self.empty_pages = None
        self.full_pages = None
        self.pages_big = None
        self.fill_20 = None
        self.fill_40 = None
        self.fill_60 = None
        self.fill_80 = None
        self.fill_99 = None
        self.re = {
            'name': re.compile('^([^ ].+) \([0-9]+\)$'),
            'table_space': re.compile('^ {4}Tablespace: (.+)$'),
            'ppp': re.compile('^ {4}Primary pointer page: ([0-9]+), Index root page: ([0-9]+)$'),
            'formats': re.compile('^ {4}Total formats: ([0-9]+), used formats: ([0-9]+)$'),
            'avg_rec_len': re.compile('^ {4}Average record length: ([0-9\.]+), total records: ([0-9]+)'),
            'avg_ver_len': re.compile(
                '^ {4}Average version length: ([0-9\.]+), total versions: ([0-9]+), max versions: ([0-9]+)$'),
            'avg_fr_len': re.compile(
                '^ {4}Average fragment length: ([0-9\.]+), total fragments: ([0-9]+), max fragments: ([0-9]+)$'),
            'pack_effect': re.compile('^ {4}Average unpacked length: ([0-9.]+), compression ratio: ([0-9\.]+)'),
            'blobs': re.compile('^ {4}Blobs: ([0-9]+), total length: ([0-9]+), blob pages: ([0-9]+)$'),
            'blobs_levels': re.compile('^ {8}Level 0: ([0-9]+), Level 1: ([0-9]+), Level 2: ([0-9]+)$'),
            'data_pages': re.compile('^ {4}Data pages: ([0-9]+), data page slots: ([0-9]+), average fill: ([0-9]+)%'),
            'pointer_pages': re.compile('^ {4}Pointer pages: ([0-9]+), data page slots: ([0-9]+)$'),
            'data_pages_fill': re.compile('^ {4}Data pages: ([0-9]+), average fill: ([0-9]+)%'),
            'primary_pages': re.compile(
                '^ {4}Primary pages: ([0-9]+), secondary pages: ([0-9]+), swept pages: ([0-9]+)$'),
            'empty_full': re.compile('^ {4}Empty pages: ([0-9]+), full pages: ([0-9]+)$'),
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
        self.tabel_space = None
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
        self.tabel_space = None
        self.depth = None
        self.leaf_buckets = None
        self.nodes = None
        self.avg_length = None
        self.dup_total = None
        self.dup_max = None
        self.avg_node_len = None
        self.avg_key_len = None
        self.compression_ratio = None
        self.avg_prefix_len = None
        self.cluster_factor = None
        self.cluster_ratio = None
        self.fill_20 = None
        self.fill_40 = None
        self.fill_60 = None
        self.fill_80 = None
        self.fill_99 = None
        self.re = {
            'name': re.compile('^ {4}Index (.+) \(([0-9]+)\)$'),
            'table_space': re.compile('^\tTablespace: (.+)$'),
            'depth': re.compile('^\t(Root page: [0-9]+, )*depth: ([0-9]+), leaf buckets: ([0-9]+), nodes: ([0-9]+)$',
                                re.IGNORECASE),
            'avg_data_len': re.compile('^\tAverage data length: ([0-9\.]+), total dup: ([0-9]+), max dup: ([0-9]+)$'),
            'avg_node_len': re.compile('^\tAverage node length: ([0-9\.]+), total dup: ([0-9]+), max dup: ([0-9]+)$'),
            'avg_key_len': re.compile('^\tAverage key length: ([0-9\.]+), compression ratio: ([0-9\.]+)$'),
            'avg_prefix_len': re.compile('^\tAverage prefix length: ([0-9\.]+), average data length: ([0-9\.]+)$'),
            'clustering_factor': re.compile('^\tClustering factor: ([0-9]+), ratio: ([0-9\.]+)$'),
            'fill_distribution': re.compile('^\tFill distribution:$'),
            'fill_0-19': re.compile('^\t {4} 0 - 19% = ([0-9]+)$'),
            'fill_20-39': re.compile('^\t {4}20 - 39% = ([0-9]+)$'),
            'fill_40-59': re.compile('^\t {4}40 - 59% = ([0-9]+)$'),
            'fill_60-79': re.compile('^\t {4}60 - 79% = ([0-9]+)$'),
            'fill_80-99': re.compile('^\t {4}80 - 99% = ([0-9]+)$')
        }

    def reset_stat(self):
        self.name = None
        self.tabel_space = None
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


class TableSpaces:
    """Class description here"""

    def __init__(self):
        """Class init"""
        self.name = None
        self.file_name = None
        self.re = {
            'name': re.compile('^([^ ]+) \(([0-9]+)\)$'),
            'file_name': re.compile('^ {4}Full path: (.+)$'),
        }

    def reset_stat(self):
        self.name = None
        self.file_name = None


class GStatToSQL:
    """Class description here"""

    def __init__(self, gstat_file, gstat_date=date.today(), dsn='', db_user=None, db_pass=None, db_charset='UTF-8',
                 db_name=''):
        """Class init description here"""
        self.gstat_file = gstat_file
        self.gstat_date = gstat_date
        self.dsn = dsn
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_charset = db_charset
        self.db_name = db_name

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
                # Skip Variable header data
                while (line != "Analyzing database pages ...\n") and (line != "Tablespaces:\n"):
                    line = fd.readline()
                break
            else:
                print("Unrecognized string at ", fd.tell())
                print(line)
                exit(10)
        if self.db_name != '':
            db_header.value['database'] = self.db_name
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
                       (db_header.value['database'], db_header.value['page_size'],
                        datetime.strptime(db_header.value['create_date'], '%b %d, %Y %H:%M:%S'),
                        self.gstat_date, db_header.value['generation'], db_header.value['ods_version'],
                        db_header.value['oldest_transaction'], db_header.value['oldest_active'],
                        db_header.value['oldest_snapshot'], db_header.value['next_transaction'],
                        db_header.value['bumped_transaction'], db_header.value['sequence_number'],
                        db_header.value['next_attachment_id'], db_header.value['implementation'],
                        db_header.value['shadow_count'], db_header.value['page_buffers'],
                        db_header.value['next_header_page'], db_header.value['database_dialect'],
                        db_header.value['attributes']))
        db_id = cursor.fetchone()[0]
        if line == "Tablespaces:\n":
            table_spaces = TableSpaces()
            line = fd.readline()
            while line != "Analyzing database pages ...\n":
                if table_spaces.re['name'].search(line):
                    table_spaces.name = table_spaces.re['name'].search(line).group(1)
                    table_spaces.id = table_spaces.re['name'].search(line).group(2)
                    line = fd.readline()
                    if table_spaces.re['file_name'].search(line):
                        table_spaces.file_name = table_spaces.re['file_name'].search(line).group(1)
                        #print(table_spaces.name, " ", table_spaces.id, " ", table_spaces.file_name)
                elif line == "\n":
                    if table_spaces.name:
                        cursor.execute(
                            "update or insert into TBL_SPC (DB_ID, \"NAME\", FILE_NAME) "
                            "values (?, ?, ?) "
                            "matching (DB_ID, \"NAME\", FILE_NAME);",
                            (db_id, table_spaces.name, table_spaces.file_name))
                        table_spaces.reset_stat()
                # Skip list of tables and index in this tablespace
                line = fd.readline()

        table = TableStat()
        tbl_id = None
        index = IndexStat()
        line = fd.readline()
        while line != '':
            # Processing table
            if table.re['name'].search(line):
                table.name = table.re['name'].search(line).group(1)
                line = fd.readline()
                if table.re['table_space'].search(line):
                    table.tabel_space = table.re['table_space'].search(line).group(1)
                    line = fd.readline()
                if table.re['ppp'].search(line):
                    line = fd.readline()
                continue
            elif table.re['formats'].search(line):
                table.formats_total = table.re['formats'].search(line).group(1)
                table.formats_used = table.re['formats'].search(line).group(2)
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
            elif table.re['pack_effect'].search(line):
                table.avg_unpack_length = table.re['pack_effect'].search(line).group(1)
                table.compress_ratio = table.re['pack_effect'].search(line).group(2)
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
            elif table.re['pointer_pages'].search(line):
                table.pointer_pages = table.re['pointer_pages'].search(line).group(1)
                table.pages_slot = table.re['pointer_pages'].search(line).group(2)
                line = fd.readline()
                if table.re['data_pages_fill'].search(line):
                    table.pages_data = table.re['data_pages_fill'].search(line).group(1)
                    table.pages_fill_avg = table.re['data_pages_fill'].search(line).group(2)
                    line = fd.readline()
                    if table.re['primary_pages'].search(line):
                        table.primary_pages = table.re['primary_pages'].search(line).group(1)
                        table.secondary_pages = table.re['primary_pages'].search(line).group(2)
                        table.swept_pages = table.re['primary_pages'].search(line).group(3)
                        line = fd.readline()
                        if table.re['empty_full'].search(line):
                            table.empty_pages = table.re['empty_full'].search(line).group(1)
                            table.full_pages = table.re['empty_full'].search(line).group(2)
                            line = fd.readline()
                continue
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
                                table.fill_80 = table.re['fill_60-79'].search(line).group(1)
                                line = fd.readline()
                                if table.re['fill_80-99'].search(line):
                                    table.fill_99 = table.re['fill_80-99'].search(line).group(1)
                                    line = fd.readline()
                continue
            # Processing index
            elif index.re['name'].search(line):
                index.name = index.re['name'].search(line).group(1)
                line = fd.readline()
                if index.re['table_space'].search(line):
                    index.tabel_space = index.re['table_space'].search(line).group(1)
                    line = fd.readline()
                if index.re['depth'].search(line):
                    index.depth = index.re['depth'].search(line).group(2)
                    index.leaf_buckets = index.re['depth'].search(line).group(3)
                    index.nodes = index.re['depth'].search(line).group(4)
                    line = fd.readline()
                    if index.re['avg_data_len'].search(line):
                        index.avg_length = index.re['avg_data_len'].search(line).group(1)
                        index.dup_total = index.re['avg_data_len'].search(line).group(2)
                        index.dup_max = index.re['avg_data_len'].search(line).group(3)
                        line = fd.readline()
                    elif index.re['avg_node_len'].search(line):
                        index.avg_node_len = index.re['avg_node_len'].search(line).group(1)
                        index.dup_total = index.re['avg_node_len'].search(line).group(2)
                        index.dup_max = index.re['avg_node_len'].search(line).group(3)
                        line = fd.readline()
                        if index.re['avg_key_len'].search(line):
                            index.avg_key_len = index.re['avg_key_len'].search(line).group(1)
                            index.compression_ratio = index.re['avg_key_len'].search(line).group(2)
                            line = fd.readline()
                            if index.re['avg_prefix_len'].search(line):
                                index.avg_prefix_len = index.re['avg_prefix_len'].search(line).group(1)
                                index.avg_length = index.re['avg_prefix_len'].search(line).group(2)
                                line = fd.readline()
                                if index.re['clustering_factor'].search(line):
                                    index.cluster_factor = index.re['clustering_factor'].search(line).group(1)
                                    index.cluster_ratio = index.re['clustering_factor'].search(line).group(2)
                                    line = fd.readline()
                    else:
                        # error
                        continue
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
                        "update or insert into TBL (DB_ID, \"NAME\", TBL_SPC, REC_AVG_LEN, REC_TOTAL, "
                        "VER_AVG_LEN, VER_TOTAL, VER_MAX, "
                        "FRAG_AVG_LEN, FRAG_TOTAL, FRAG_MAX, "
                        "BLOB_TOTAL, BLOB_TOTAL_LENGTH, BLOB_PAGES, "
                        "BLOB_LEVEL1, BLOB_LEVEL2, BLOB_LEVEL3, "
                        "PAGES_DATA, PAGES_SLOT, PAGES_FILL_AVG, PAGES_BIG, "
                        "POINTER_PAGES, PRIMARY_PAGES, SECONDARY_PAGES, SWEPT_PAGES, "
                        "EMPTY_PAGES, FULL_PAGES, "
                        "FILL_20, FILL_40, FILL_60, FILL_80, FILL_99, "
                        "FORMATS_TOTAL, FORMATS_USED, AVG_UNPACK_LEN, COMPRESS_RATIO) "
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                        "?, ?, ?, ?, ?, ?, ?) "
                        "matching (DB_ID, \"NAME\") "
                        "returning ID;",
                        (db_id, table.name, table.tabel_space, table.rec_avg_len, table.rec_total,
                         table.ver_avg_len, table.ver_total, table.ver_max,
                         table.frag_avg_len, table.frag_total, table.frag_max,
                         table.blob_total, table.blob_total_length, table.blob_pages,
                         table.blob_level0, table.blob_level1, table.blob_level2,
                         table.pages_data, table.pages_slot, table.pages_fill_avg, table.pages_big,
                         table.pointer_pages, table.primary_pages, table.secondary_pages, table.swept_pages,
                         table.empty_pages, table.full_pages,
                         table.fill_20, table.fill_40, table.fill_60, table.fill_80, table.fill_99,
                         table.formats_total, table.formats_used, table.avg_unpack_length, table.compress_ratio))
                    tbl_id = cursor.fetchone()[0]
                    table.reset_stat()
                elif index.name:
                    # insert data about index
                    cursor.execute(
                        "update or insert into IDX (DB_ID, TBL_ID, \"NAME\", TBL_SPC, DEPTH, LEAF_BUCKETS, NODES, "
                        "AVG_LENGTH, DUP_TOTAL, DUP_MAX, AVG_NODE_LEN, AVG_KEY_LEN, "
                        "COMPRESSION_RATIO, AVG_PREFIX_LEN, CLUSTER_FACTOR, CLUSTER_RATIO, "
                        "FILL_20, FILL_40, FILL_60, FILL_80, FILL_99) "
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        "matching (DB_ID, TBL_ID, \"NAME\");",
                        (db_id, tbl_id, index.name, index.tabel_space, index.depth, index.leaf_buckets, index.nodes,
                         index.avg_length, index.dup_total, index.dup_max, index.avg_node_len, index.avg_key_len,
                         index.compression_ratio, index.avg_prefix_len, index.cluster_factor, index.cluster_ratio,
                         index.fill_20, index.fill_40, index.fill_60, index.fill_80, index.fill_99)
                    )
                    index.reset_stat()
            elif re.search('^Gstat completion time ', line):
                pass
            elif re.search('^    Expected data on page [0-9]+$', line):
                pass
            else:
                print("Unrecognized string at ", fd.tell())
                print(line)
                connection.rollback()
                exit(11)
            line = fd.readline()
        connection.commit()
        fd.close()


if __name__ == '__main__':

    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('gstat_file', metavar='file', type=str, nargs=1, help='file with gstat output')
    parser.add_argument('--date', metavar='date', type=str, default=date.today(),
                        help='date of statistics collection, default today')
    parser.add_argument('--user', metavar='user', type=str, default='SYSDBA', help='DB user, default SYSDBA')
    parser.add_argument('--passwd', metavar='passwd', type=str, default='masterkey',
                        help='DB password, default masterkey')
    parser.add_argument('--dsn', metavar='dsn', type=str, default='localhost:gstat',
                        help='DB connection string, default localhost:gstat')
    parser.add_argument('--db_name', metavar='db_name', type=str, default='',
                        help='DB name in statistics')
    args = parser.parse_args()

    gstat = GStatToSQL(gstat_file=args.gstat_file[0], gstat_date=args.date, dsn=args.dsn, db_user=args.user,
                       db_pass=args.passwd, db_name=args.db_name)
    gstat.processing()

    exit(0)
