#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# A not-so-goood data load utility created by John Wang(john_wang07@infosys.com) in Oct/2012
#

import argparse, os, sys, glob, datetime, csv, re, traceback, codecs

SUPPORTED_DBS = ['oracle','sqlite3','msaccess']

def get_args():
    parser = argparse.ArgumentParser(description='A tool loading data from a set of CSV files into database. Created by John Wang(john_wang07@infosys.com)',
                            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    def _exit_w_info(info):
        print '\n%s\n' % info
        parser.print_help()
        sys.exit(0)
    
    db_opts = parser.add_argument_group('Database Options')
    db_opts.add_argument('-l','--db_dialect', choices=SUPPORTED_DBS,
                   default='sqlite3', 
                   help="The target database engine.")
    db_opts.add_argument('-d','--db_host', default='localhost', help="The target database server address. If a file database engine(e.g., sqlite3 or msaccess) is used, the value should be the path to the file.")
    db_opts.add_argument('-o','--db_port',type=int, default=1521, help="The port the target database server is listening on. If sqlite_db is specified, this option will be ignored.")
    db_opts.add_argument('-s','--db_sid', help="The database server sid. If sqlite_db is specified, this option will be ignored.")
    db_opts.add_argument('-u','--db_user',help="The user name used to connect to the database server. If sqlite_db is specified, this option will be ignored.")
    db_opts.add_argument('-p','--db_pwd',help="The user password used to connect to the database server. If sqlite_db is specified, this option will be ignored.")
    db_opts.add_argument('-e','--db_encoding', default='utf8', help="The encoding the DB driver expects for the value to be inserted.")
    
    mp_opts = parser.add_argument_group('Mapping Configure Options')
    mp_opts.add_argument('-m','--mapping', default="mapping.csv",
                   help="The path of the mapping configure file.")
    mp_opts.add_argument('-f','--file_path',help="The data file path from where the data been loaded.")
    mp_opts.add_argument('-w','--raw_date_fmt', default='%m/%d/%Y %I:%M:%S %p', help='The date format used to parse the source data of datetime type from the CSVs. You need to quote the parameter if it has spaces in it.')
    mp_opts.add_argument('csvs', metavar='csv', nargs='*',help="The CSV files to be loaded into the Database.")
    
    exe_opts = parser.add_argument_group('Execute Options')
    exe_opts.add_argument('-x','--execute', action="store_true", 
                   help="Execute the SQL in the database instead of print them out.")
    exe_opts.add_argument('-t','--create_table', action="store_true", 
                   help="If provided, the table will be created before the records being inserted.")
    exe_opts.add_argument('-a','--batch_insert', action="store_true", help='If present, inserting to table will be executed in batch mode.')
    exe_opts.add_argument('-v','--verbose', action="store_true", help='If present, detailed processing(log) information will be shown on the screen.')
    exe_opts.add_argument('-k','--pk_base', type=int, default=1000, help="The starting value of generated primary keys for a database record.")
    
    
    
    args = parser.parse_args()
    if args.file_path:
        args.csvs.extend(glob.glob(args.file_path))
    if not os.path.isfile(args.mapping):
        _exit_w_info('Error: mapping configure not exists: "%s". You can specify a valid mapping file by the "-m" option' % args.mapping)
    if args.db_dialect == SUPPORTED_DBS[2]: # Validating MS Access database options
        if os.path.isfile(args.db_host):
            args.db_host = os.path.abspath(args.db_host) # Seemed like MS Access only support absolute path.
        else:
            _exit_w_info('Error: MS Access Database file "%s" not found .' % args.db_host)
    if not args.csvs:
        _exit_w_info('Error: no data(csv) file found. Please specify valid CSV files either by the "-f" option or the positional argument.')
    print 'Commandline args:\n', vars(args)
    return args

RAW_ID_NAME = '#orid'
class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self
        
def load_csv(fpath,mapping):
    '''Load data from the source CSV file. Return a the CSV name and a list of 2-tuples list as the following:
    [
        [(field_name_1, value_1), ... (field_name_n, value_n)],
        [(field_name_1, value_1), ... (field_name_n, value_n)],
        ... ...
    ]
    '''
    print 'loading "%s" ...' % fpath
    fname, _, ext=os.path.basename(fpath).rpartition('.')
    fname = (fname or ext).lower()
    rt = None
    if fname in [mp[1] for mp in mapping]:
        with open(fpath,'rb') as f:
            reader = UnicodeReader(f)
            headers = [h.lower() for h in reader.next()]
            rows = [zip(headers,[c.strip() for c in row]) for row in reader if ''.join(row).strip()]
            if args.verbose: print 'raw csv data:\n', rows[:2]
            used_fields = set(sum([[fmp[3].partition(':')[0] for k, fmp in mp[2:]]
                                    for mp in mapping if mp[1]==fname], []))
            if args.verbose: print 'used fields:', used_fields
            rt = [[(k,v) for k,v in row if k in used_fields] for row in rows]
            rt = [r + [(RAW_ID_NAME, idx)] for idx, r in enumerate(rt)]
            if args.verbose: print 'loaded csv data:\n', rt[:2]
    else:
        print 'file "%s" not in mapping configure, skipped.' % fname
        return None
    if args.verbose: print 'loaded csv data:\n', rt[:2]
    return fname,rt

def load_mapping(mfile):
    '''Parse the mapping configure. Return the a list with following structure:
    [ 
        (
            table_name,
            source_csv,
            (field_name_1, [field_type, default_value, vmap, source_field]),
            ... ...
            (field_name_n, [field_type, default_value, vmap, source_field]),
        ),
        (
            table_name,
            source_csv,
            (field_name_1, [field_type, default_value, vmap, source_field]),
            ... ...
            (field_name_n, [field_type, default_value, vmap, source_field]),
        ),
    ]
    '''
    mapping=[]
    def pop_map(mp_row):
        t_name, tf_name, tf_type, tf_default, vmap, s_name, sf_name = [v.lower() for v in mp_row]
        if not tf_type:
            print 'Error: The "Type" of the field "%s" not specified for table "%s".' % (tf_name, t_name or mapping[-1][0])
            sys.exit(0)
        if tf_type == 'int' and tf_default:
            try:int(tf_default)
            except:
                print 'Error: The default value "%s" is not a valid integer for field "%s" of table "%s".' % (tf_default,tf_name, t_name or mapping[-1][0])
                sys.exit(0)
        map_attr = [tf_type,tf_default,vmap,sf_name]
        if len(mapping) == 0 or (t_name and t_name != mapping[-1][0]):
            mapping.extend([[t_name, s_name, [tf_name, map_attr]],])
        else:
            mapping[-1].append([tf_name,map_attr])
    with open(mfile,'rb') as f:
        reader=UnicodeReader(f)
        reader.next() #Ignore the headers
        tmp=[pop_map([c.strip() for c in row]) for row in reader if ''.join(row).strip()]
    return mapping

def _trac_error(fn):
    def _fn(*ag, **kag):
        try:
            return fn(*ag, **kag)
        except:
            print 'Error when invoke function "%s" with args:\n\t%s\n\t%s.' %(fn.__name__, ag, kag)
            if args.verbose: traceback.print_exc()
            sys.exit(0)
    return _fn

def _m2m_table(ttable, fmp):
    m2_ref_tbl, _, m2field = fmp[2].partition('.')
    mid_tbl = '%s_%s_map' % (ttable,m2_ref_tbl)
    mid_id_fname = '%s_id' % mid_tbl
    mid_m_fname = '%s_id' % ttable
    mid_m2_fname = '%s_id' % m2_ref_tbl
    return mid_tbl, mid_id_fname, mid_m_fname, mid_m2_fname
    
def _mid_tables(mapping):
    mid_tbls = []
    for mp in mapping:
        table_name=mp[0]
        m2m_fields = [(k,v) for k,v in mp[2:] if v[0]=='m2m']
        mid_tbls.extend([_m2m_table(table_name, fmp) for _,fmp in m2m_fields])
    return mid_tbls
    
def _parse_map(mp):
    return mp[0], mp[1], mp[2:]
    
def resolve(raw_data, mapping):
    '''Resolve the raw data raw_data from CSV according to the configure mapping, and generate the clean data to be inserted into database. raw_data is a dict with the key being the CSV file name and the value being the data rows loaded from the CSV file. 
    return a list of 2-tuples for different tables, in which the 1st is the SQL statement and the 2nd is the clean data, as the following:
    [
        (TABLE_SQL_1, [
                        {field_1:value_1, ..., field_n:value_n},
                        {field_1:value_1, ..., field_n:value_n},
                        ... ...
                        ])
        (TABLE_SQL_2, [
                        {field_1:value_1, ..., field_n:value_n},
                        {field_1:value_1, ..., field_n:value_n},
                        ... ...
                        ])
            ... ...
    ]
    '''
    NOW_TIME = datetime.datetime.now()
    FIELDS_SEP = '$csep$' #The seperator among fields
    KV_SEP = '$ksep$' #The seperator between field name and its value
    
    def _gen_pk(x):
        _gen_pk.pkseq += 1
        return _gen_pk.pkseq
    _gen_pk.pkseq = args.pk_base
    
    def _default_text(x):
        return x or ''
    
    def _default_int(x):
        return (x and int(x)) or 0
        
    def _default_datetime(x):
        return (x and datetime.datetime.strptime(x,args.raw_date_fmt)) or NOW_TIME
    
    def _raw_value(s_field,rrow):
        sf, _, rex = s_field.partition(':')
        rval = dict(rrow).get(sf,'')
        mm = rex and re.match(rex,rval,re.I)
        return (mm and mm.groups() and mm.groups()[0]) or rval
        
    def _conv_text(fmp, raw_row, result_dict):
        return _raw_value(fmp[3], raw_row) or _default_text(fmp[1])
    
    def _conv_int(fmp, raw_row, result_dict):
        rval=_raw_value(fmp[3], raw_row)
        raw_value = re.sub(r'[ ,.]', '', rval)
        raw_value = 0 if (not raw_value) or raw_value.upper() in ('NULL', 'NONE', 'NIL') else raw_value
        try:
            return 0 if raw_value == 0 else (raw_value and int(raw_value)) or _default_int(fmp[1])
        except:
            if args.verbose:
                print "Failed in converting to int: %s. Default: %s used" % (raw_value, fmp[1])
                traceback.print_exc()
            return _default_int(fmp[1])
    
    def _conv_datetime(fmp, raw_row, result_dict):
        raw_value=_raw_value(fmp[3], raw_row)
        return (raw_value and datetime.datetime.strptime(raw_value, args.raw_date_fmt)) or _default_datetime(fmp[1])
    
    def _conv_m2o(fmp, raw_row, result_dict):
        oend_table, _, oend_field = fmp[2].partition('.')
        oend_field, _, oend_cfield = oend_field.partition('+')
        cfield_name, _, cfield_value = oend_cfield.partition('=')
        def _ffunc(trow):#Check if the row is the related one.
            sf_val = dict(raw_row).get(fmp[3].partition(':')[0],None)
            mvm = (dict(trow)[oend_field] == sf_val)
            return (mvm and dict(trow)[cfield_name] == cfield_value) if cfield_name else mvm
        rt = [trow[0] and trow[0][1] or None for trow in result_dict[oend_table] if _ffunc(trow)] #Get the pk of the related row
        return rt[0] if rt else None

    VALUE_MAPS={
        'pk':       (_gen_pk, None),
        'text':     (_default_text, _conv_text),
        'int':      (_default_int, _conv_int),
        'datetime': (_default_datetime, _conv_datetime),
        'm2o':      (lambda x:None, _conv_m2o),
        'm2m':      (lambda x:None, _conv_m2o),
    }
    
    def _resolve(fmp, raw_row,result_dict):
        default_func, convert_func = VALUE_MAPS[fmp[0]]
        value = (fmp[3] and convert_func(fmp,raw_row,result_dict)) or default_func(fmp[1])
        return value
                
    def _retros_tbl_map(table_name,mapping):
        u'''return a structure as "[(fmp, csv), ..., (fmp, csv)]"'''
        return sum([[(fmp,mp[1]) for f,fmp in mp[2:] if fmp[2].startswith(table_name)] for mp in mapping],[])
                
    def _parse_vmap(vmap,rval):
        tbl_mfield, _, cat = vmap.partition('+')
        _,_,main_fieldname = tbl_mfield.partition('.')
        if cat:
            cat_fieldname,_,cat_val = cat.partition('=')
        return (main_fieldname, rval), (cat and (cat_fieldname,cat_val)) or []
            
    rt = {}
    for mp in mapping: # 1st round data resolve
        table_name, csv_name, field_maps = _parse_map(mp)
        if args.verbose: print '1st round resolving, target table: %s, csv file: %s, field maps: %s .' %(table_name, csv_name, field_maps)
        if csv_name:
            raw_rows = raw_data[csv_name]
            if args.verbose: print 'raw rows:\n', raw_rows[:2]
            if not any([fmp[2] for _, fmp in field_maps]): #For dictionary table with source
                str_rows = set([FIELDS_SEP.join([sf+KV_SEP+ dict(rrow).get(sf,'')
                                        for sf in [fmp[3].partition(':')[0] for _,fmp in field_maps if fmp[3]]])
                              for rrow in raw_rows if rrow])
                rrows = [[cs.split(KV_SEP) for cs in row.split(FIELDS_SEP)] for row in str_rows]
                raw_rows = [[cell for cell in rr if len(cell)>1] for rr in rrows]
                raw_rows = [rrow+[(RAW_ID_NAME,idx)] for idx, rrow in enumerate(raw_rows)]
            resolved_data = [[(f, _resolve(fmp,rrow,rt)) for f, fmp in field_maps]+[rrow[-1]] for rrow in raw_rows]
        else: # for dictionary table without direct source, we restropect all the source data and collect distinct values.
            # [(t_field, fmp, csv), ..., (t_field, fmp, csv)]
            field_ref_rvals = sum([[(fmp[2],dict(rrow).get(fmp[3].partition(':')[0],'')) for rrow in raw_data[csv] if rrow]
                                    for fmp, csv in _retros_tbl_map(table_name,mapping)],[])
            tmp_data = [_parse_vmap(vmap,rval) for vmap, rval in field_ref_rvals]
            #There may be duplicated data in the raw source. Put it through a "set" to distinct them. 
            data_set = set([FIELDS_SEP.join([KV_SEP.join(mcell),KV_SEP.join(ccell)]) for mcell,ccell in tmp_data])
            data_list = [[cell.split(KV_SEP) for cell in v.split(FIELDS_SEP)] for v in data_set]
            field_pk = [f for f, m in field_maps if m[0]=='pk'][0]
            resolved_data = [[(field_pk, args.pk_base+idx)]+[c for c in cells if c and len(c)>1] + [(RAW_ID_NAME,idx)] \
                                    for idx, cells in enumerate(data_list)]
        rt.update({table_name:resolved_data})
    
    for mp in mapping: # 2nd round resolving, generate M2M table
        table_name=mp[0]
        m2m_fields = [(k,v) for k,v in mp[2:] if v[0]=='m2m']
        if m2m_fields:
            for f,fmp in m2m_fields:
                m2_ref_tbl, _, m2field = fmp[2].partition('.')
                mid_tbl = '%s_%s_map' % (table_name,m2_ref_tbl)
                mid_id_fname = '%s_id' % mid_tbl
                mid_m_fname = '%s_id' % table_name
                mid_m2_fname = '%s_id' % m2_ref_tbl
                table_data = rt[table_name]
                mid_data = [[(mid_id_fname, args.pk_base+i),(mid_m_fname,row[0][1]), (mid_m2_fname,dict(row)[f])] 
                                for i, row in enumerate(table_data)]
                rt[mid_tbl] = mid_data
            todb_data = [[(fname,fval) for fname,fval in row if fname not in [k for k,v in m2m_fields]]
                                for row in rt[table_name]] #since the m2m mid table is resolved, we then remove the m2m field in the related table
            rt[table_name] = todb_data
    return dict([(tbl, [filter(lambda x:x[0]!=RAW_ID_NAME, r) for r in rows]) for tbl,rows in rt.items()]) #remove the fake field before reture

def _gen_standard_sql(table,row1):
    return u'INSERT INTO %s (%s) VALUES (%s)' % \
                (table, ','.join([k for k,_ in row1]), ','.join([':c%s'%i for i,_ in enumerate(row1)]))
def _gen_msaccess_sql(table,row1):
    return u'INSERT INTO %s (%s) VALUES (%s)' % \
                (table, ','.join([k for k,_ in row1]), ','.join(['?' for _ in row1]))
    
def _gen_sql(table, row1):
    return dict(zip(SUPPORTED_DBS, [_gen_standard_sql, _gen_standard_sql, _gen_msaccess_sql]))[args.db_dialect](table, row1)
    
def _execute(table,rows,conn,cursor):            
    sql = _gen_sql(table, rows[0])
    if args.verbose:
        print 'Running SQL:\n', sql, '\ndata:\n',rows[:1]
    if args.batch_insert:
        cursor.executemany(sql,[[v for _,v in r] for r in rows])
    else:
        for r in rows:
            row = [v for _,v in r]
            try:
                cursor.execute(sql, row)
            except:
                print 'Error when inserting row: %s.' % row
                sys.exit(0)
    conn.commit()
    
def _print_stat(table, rows):
    sql = _gen_sql(table, rows[0])
    print '==== %s (%d rows) ======' % (table,len(rows))
    print sql
    print '---------------------------------------------'
    print '|'.join([" " + k.ljust(max(len(k),10)+1) for k,_ in rows[0]]) # Table Headers
    print '\n'.join(['|'.join([" " + (str(v)[:10]).ljust(max(len(k),10)+1) for k,v in row]) for row in rows[:2]]), '\n' # some rows

# map between configure type and field type
# SUPPORTED_DBS = ['oracle','sqlite3','msaccess']
DBTMAP = dict(zip(SUPPORTED_DBS, 
    (dict((('pk', 'number'),('m2o','number'),('text','varchar2(256)'),('int','number'),('datetime','timestamp'),('m2m','number'))),
    dict((('pk', 'int'),('m2o','int'),('text','text'),('int','int'),('datetime','text'),('m2m','int'))),
    dict((('pk', 'LONG'),('m2o','LONG'),('text','VARCHAR'),('int','LONG'),('datetime','DATETIME'),('m2m','LONG'))),)))

    
def _gen_ddl(tbl,field_defs):
    if not field_defs:return
    pk_def = '%s %s PRIMARY KEY' % field_defs[0]
    fds = pk_def + ', ' + ', '.join([f+' '+t for f,t in field_defs[1:]])
    return 'CREATE TABLE %s (%s)' %(tbl,fds)
    
def gen_mid_ddl(mid_tbl, mid_id_fname, mid_m_fname, mid_m2_fname):
    return _gen_ddl(mid_tbl, map(lambda x:(x,DBTMAP[args.db_dialect]['int']), [mid_id_fname, mid_m_fname, mid_m2_fname]))
       
def _map_dbtype(otype):
    return DBTMAP[args.db_dialect][otype]

def _gen_fdefs(tbl, row, mapping):
    mps = filter(lambda x:x[0]==tbl, mapping)
    if mps:
        field_maps = dict(_parse_map(mps[0])[2])
        return [(f,_map_dbtype(field_maps[f][0])) for f,_ in row]

def gen_ddls(table_data, mapping):
    rt = [_gen_ddl(tbl, _gen_fdefs(tbl, rows[0], mapping)) for tbl, rows in table_data.items()]
    return filter(lambda x:x, rt) #remove blank element

def _conn_oracle():
    import cx_Oracle as oracle
    os.environ["NLS_LANG"] = ".%s"%args.db_encoding.upper()
    return oracle.connect(args.db_user, args.db_pwd, oracle.makedsn(args.db_host,args.db_port,args.db_sid))
    
def _conn_sqlite3():
    import sqlite3
    return sqlite3.connect(args.db_host)
    
def _conn_msaccess():
    import pyodbc
    return pyodbc.connect("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;" % args.db_host)
    
def _conn():
    return dict(zip(SUPPORTED_DBS, [_conn_oracle, _conn_sqlite3, _conn_msaccess]))[args.db_dialect]()
    
def dump_data(table_data,mapping):
    if args.execute:
        if args.verbose:print 'Connecting Database ...'
        conn = _conn()
        cs = conn.cursor()
        if args.create_table:
            ddls = gen_ddls(table_data, mapping) + [gen_mid_ddl(*ff) for ff in _mid_tables(mapping)]
            for ddl in ddls: #Drop tables first
                table_name = ddl.split()[2]
                if args.verbose:print 'Droping Table:', table_name
                try:
                    cs.execute('DROP TABLE %s' % table_name)
                except:#Simply ignore the error if the table is not there.
                    if args.verbose:print 'Error when drop the table "%s"' % table_name
            conn.commit()
            for ddl in ddls: #Create Tables
                if args.verbose:print 'Creating Table:\n', ddl
                cs.execute(ddl)
            conn.commit()
        for table, rows in table_data.items():
            print 'Dumping %s rows into table "%s" ...' %(len(rows), table)
            _execute(table,rows,conn,cs) 
        conn.close()
    else:
        if args.create_table:
            ddls = gen_ddls(table_data, mapping) + [gen_mid_ddl(*ff) for ff in _mid_tables(mapping)]
            print '==== DDLs =============='
            for ddl in ddls:
                print ddl,'\n---------------------------------'
        for table, rows in table_data.items():
            _print_stat(table,rows)
            
if __name__ == '__main__':
    global args
    args= get_args()
    print 'Loading mapping configure ...'
    mapping = load_mapping(args.mapping)
    print 'Load source data from CSV ...'
    raw_data = filter(lambda x:x, [load_csv(f,mapping) for f in args.csvs])
    print 'Resolving data ...'
    table_data = resolve(dict(raw_data), mapping)
    print 'Dumping into database ...'
    dump_data(table_data, mapping)
    print 'Done !'