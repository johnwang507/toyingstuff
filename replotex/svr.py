#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from bottle import request, response, HTTPResponse, get, post, redirect, run, view, static_file
import sys, json, re, collections, itertools, copy, traceback
from datetime import datetime, timedelta

RDM_DB = 'host=db.s dbname=rdm_dev user=postgres'

# ISS_URL = '/issues'
RECALL_DAYS = 90 # Raise error if you try to recall issues back beyond this many days
CHART_TYPES = ["bd","burndown","line","bar","pie","stackline","stackbar"]

CACHE_QRT_LMT = 20 # How many query result can be cached 
CACHE_QRT_KEY = 'QRT' # query result cache key
ENUM_CACHE_EXPIRE = 3 * 24 * 3600 # Cache synchronize interval(seconds).

# Regx and JavaScript parsing functions data_types: 0--int, 1--date, 2--float
VAL_RGX = [(r'^\d+$', 'parseInt'), (r'^(\d\d){1,4}$','normalize_date'), (r'^\d*\.?\d*$','parseFloat')]
DIM_LST = [
    #0id  1name_en     2name_cn        3filter_key        4enum?  5type  6comp? 7step  8jsonFn       9synCacheFn
    (1,  "author",     "作者",         "author_id",        1,      0,     0 ,     0,   'jsn_plain',  'syn_user'),
    (2,  "created",    "创建于",       "created_on",       0,      1,     1 ,     1,    0,            0),
    (3,  "updated",    "更新于",       "updated_on",       0,      1,     1 ,     1,    0,            0),
    (4,  "project",    "项目",         "project_id",       1,      0,     0 ,     0,    'jsn_prj',   'syn_prj'),
    (5,  "category",   "类别",         "category_id",     1,       0,     0 ,     0,    'jsn_cate',  'syn_cate'),
    (6,  "tracker",    "跟踪",         "tracker_id",       1,      0,     0 ,     0,    'jsn_plain', 'syn_trk'),
    (7,  "status",     "状态",         "status_id",        1,      0,     0 ,     0,    'jsn_plain', 'syn_stu'),
    (8,  "priority",   "优先级",       "priority_id",      1,      0,     1 ,     1,    'jsn_plain', 'syn_prio'),
    (9,  "assignee",   "指派给",       "assigned_to_id",   1,      0,     0 ,     0,    'jsn_mem',   'syn_mem'),
    (10, "version",    "目标版本",     "fixed_version_id", 1,      0,      0 ,    0,    'jsn_ver',   'syn_ver'),
    (11, "start",      "开始日期",     "start_date",       0,      1,      1 ,    1,    0,            0),
    (12, "due",        "计划完成日期",  "due_date",        0,      1,      1,      1,    0,           0),
    (13, "estimated",  "预期时间",     "estimated_hours",  0,      2,      1 ,    2,    0,            0),
    (14, "done",       "进度",         "done_ratio",       1,      0,     1 ,    10,    0,            0),
    (15, "day",        "历史日期",      "theday",          0,      1,     1 ,     1,    0,            0)
    ]
COMPACT_DT_FMT = '%y%m%d'
js_dims = json.dumps(map(lambda x:[x[0],x[1],x[2],x[4],x[5],x[6],], DIM_LST))
js_valRgxes = json.dumps(VAL_RGX)
js_chartypes = json.dumps(CHART_TYPES)
# dims_txt = ','.join(map(lambda x:'%s(%s)' %(x[1],x[2]), DIM_LST))
global_tpl_vars = dict(q='', dims=js_dims, 
                charTypes=js_chartypes, valRgxs=js_valRgxes)

gcache={CACHE_QRT_KEY:{}}

def pick_one(ffn, theseq):
    ft = [ele for ele in theseq if ffn(ele)]
    return ft and ft[0] or None

def time_elapsed(frm):
    td = datetime.today() - frm
    return td.total_seconds()
def _find_dim(dim_id):
    return filter(lambda x:x[0]==dim_id, DIM_LST)[0]

def _iseq(val):return (not isinstance(val, basestring)) and isinstance(val, collections.Sequence)

def _str_dimv(dimv,strfn=None,rangefn = None): 
    _strfn = strfn or (lambda x:str(x))
    _rangefn = rangefn or (lambda x:'%s~%s' % tuple(x)) #default to creating readable form for UI
    if _iseq(dimv):
        return _rangefn([_strfn(v) for v in dimv])
    return _strfn(dimv)

def _get_cache_entry(dimid):
    syn_fn = _find_dim(dimid)[9]
    _entry = gcache.get(dimid,None)
    if not _entry or (time_elapsed(_entry[1]) > ENUM_CACHE_EXPIRE):
        _entry = eval(syn_fn)()
    else:
        _entry = _entry[0]
    return _entry

def q_db(sql_select, cur_handler=list):
    # print sql_select
    import psycopg2
    try:
        connect = psycopg2.connect(RDM_DB)
        cur = connect.cursor()
        cur.execute(sql_select)
        return cur_handler(cur)
    finally:
        if cur:cur.close(); connect.close()

# Synchronize Cache ...
def _idx_cleanser(idxes=(0,1)):
    def vfn(vv):
        return (_iseq(idxes) and [vv[i] for i in idxes]) or vv[idxes]
    return lambda lst: [vfn(val) for val in lst]
def _get_cached_data(ck,synfn):
    cdata = gcache.get(ck,None)
    return cdata and cdata[0] or synfn()
def _syn_prj_enum(cky, tbl, prjid_idx, cleanser):
    cached_prjs = _get_cached_data(4, syn_prj)
    rows = q_db('SELECT * FROM %s' % tbl)
    def cache_of(prjid):
        cch = cleanser([row for row in rows if row[prjid_idx]==prjid])
        cch.sort(reverse=True)
        return cch
    cache_data = dict([(prj[0], cache_of(prj[0])) for prj in cached_prjs])
    gcache[cky] = [cache_data, datetime.today()]
    return cache_data
def _sync(enum_tbl, cky, cleanser, filterfn=None):
    rt = q_db('SELECT * FROM ' + enum_tbl)
    if not rt:return None
    if filterfn: rt = [row for row in rt if filterfn(row)]
    cache_data = cleanser(rt)
    cache_data.sort(reverse=True)
    gcache[cky] = [cache_data, datetime.today()]
    return cache_data
def syn_user():
    def _cln(lst): # id, login, last_name+first_name
        return [(user[0], user[1], user[4]+user[3]) for user in lst]
    return _sync('users', 1, _cln)
def syn_prj():#take id, identifier, name and parent_id
    return _sync('projects', 4, _idx_cleanser((0, 8, 1, 5)))
def syn_trk():
    return _sync('trackers', 6, _idx_cleanser())
def syn_stu():
    return _sync('issue_statuses', 7, _idx_cleanser((0,1,2))) #take id,name,is_closed
def syn_prio():
    return _sync('enumerations', 8, _idx_cleanser(), filterfn=(lambda row:row[4]=='IssuePriority'))
def syn_mem():
    return _syn_prj_enum(9, 'members', 2, _idx_cleanser(1)) # take only "user_id"
def syn_cate():
    return _syn_prj_enum(5, 'issue_categories', 1, _idx_cleanser((0,2))) #take "id" and "name"
def syn_ver():
    return _syn_prj_enum(10, 'versions', 1, _idx_cleanser((0,2,4,5))) #take "id","name", "effective_date" and "created_on"

def jsn_plain(cache_entry, prj_id):
    return json.dumps(cache_entry)
def jsn_prj(cache_entry, prj_id):
    return json.dumps([(pid, pnm) for pid, _, pnm, _ in cache_entry])
def jsn_cate(cache_entry, prj_id):
    return json.dumps(prj_id and cache_entry[prj_id] or [])
def jsn_mem(cache_entry, prj_id):
    def get_u(uid):
        return [en[1:] for en in gcache[1][0] if en[0]==uid][0]
    return json.dumps(prj_id and [[uid] + list(get_u(uid)) for uid in cache_entry[prj_id]] or [])
def jsn_ver(cache_entry, prj_id):
    return json.dumps([ver[:2] for ver in cache_entry[prj_id]])
def json_enum(dimid, prjid):
    json_fn, syn_fn = _find_dim(dimid)[8:]
    if dimid == 14: # For "done_ratio"
        return json.dumps(range(110)[::10])
    _entry=_get_cache_entry(dimid)
    return eval(json_fn)(_entry, prjid)

def get_cache(dimid, prjid=0):
    _entry=_get_cache_entry(dimid)
    if prjid: return _entry.get(prjid, None)
    else: return _entry

def _dim_val(v, dim_def):
    '''Transform the raw value to the right datatype for inner usage '''
    return dim_def[5] == 1 and datetime.strptime(v, COMPACT_DT_FMT) or v

def _iscat(iss, cval, cdim_def):
    '''Does issue "iss" belong to the category "cval" ?

    >>> dim_def = (8,"priority","优先级","priority_id",1,0,1,1)
    >>> _iscat({'priority_id': 2}, 2, dim_def)
    True
    >>> _iscat({'priority_id': 2}, 3, dim_def)
    False
    >>> dim_def = (8,"priority","优先级","priority_id",1,0,1,2)
    >>> _iscat({'priority_id': 3}, 2, dim_def)
    True
    >>> dim_def = (8,"priority","优先级","priority_id",1,0,1,0)
    >>> _iscat({'priority_id': 2}, 3, dim_def), _iscat({'priority_id': 2}, 2, dim_def)
    (False, True)
    '''
    diff = _dim_val(iss[cdim_def[3]], cdim_def) - _dim_val(cval, cdim_def)
    if cdim_def[5]==1: diff = diff.days
    if cdim_def[7]:
        return (diff < cdim_def[7] and diff >= 0)
    return diff==0

def stretch(dim_def, begin, end): 
    '''Build ticks in between.

    >>> lst = stretch(DIM_LST[1], "130221","130302")
    >>> len(lst), lst[0], lst[-1]
    (10, '130221', '130302')
    >>> stretch(DIM_LST[7], 0,4)
    [0, 1, 2, 3, 4]
    '''
    is_date = dim_def[5] == 1 
    b_val = (is_date and datetime.strptime(begin, COMPACT_DT_FMT) #If user specified a begin value not right on the scale, use its ceiling instead,
                or (begin // dim_def[7] + ((begin % dim_def[7]) and 1 or 0)) * dim_def[7]) # dim_def[7] is the scale step value
    diff = is_date and (datetime.strptime(end, COMPACT_DT_FMT) - b_val).days or end-b_val
    tick_span = range(diff // dim_def[7] + 1) #Plus 1 to include the end boundery
    tick_fn = (is_date and (lambda b,i:(b + timedelta(days=(dim_def[7] * i))).strftime(COMPACT_DT_FMT))
                or (lambda b,i:b + dim_def[7] * i))
    return [tick_fn(b_val, i) for i in tick_span]

def _scales(dim_def, vals):
    range_val = filter(_iseq, vals) #The rule: range OVER individual values
    scales = range_val and stretch(dim_def, *range_val[0]) or [_dim_val(v, dim_def) for v in vals]
    scales = list(set(scales))
    scales.sort()
    return scales 

def justify4bd(qdict):
    '''For burndown, we need to remove irrelevant dimensions and add default ones'''
    justed = dict(qdict.get('if', {}))
    ons = qdict.get('on', None)
    if ons: justed.update([ons]) # rule : "on" part over "if" part
    prj_entry = pick_one(lambda x:x[0]==4, justed.items())
    prjid = prj_entry and prj_entry[1] and prj_entry[1][0] or 28 # Use "tsp-demo-100" as the default "project". Only one project is allowed
    rt = {'if':[[4, [prjid]],], 'c':'bd'}
    ver_entry = pick_one(lambda x:x[0]==10, justed.items())
    def ver_pair(ver_id=0):
        cached_vers = get_cache(10, prjid)
        if not cached_vers: raise Exception(u'No version defined for project "%s"' % pick_one(lambda x:x[0]==prjid, get_cache(4)) [2])
        if ver_id:
            idx, ver = pick_one(lambda x:x[1][0]==ver_id, enumerate(cached_vers))
            return idx<len(cached_vers)-1 and cached_vers[idx+1] or None, ver
        return len(cached_vers)>1 and cached_vers[1] or None, cached_vers[0]
    if not ver_entry:# Set the latest version of the project as the default "version"
        pre_ver, ver = ver_pair()
    else:
        ver_id = _iseq(ver_entry[1][0]) and ver_entry[1][0][0] or ver_entry[1][0]
        pre_ver, ver = ver_pair(ver_id)
    rt['if'].append([10, [ver[0]]])
    day_begin = (pre_ver and pre_ver[2] and pre_ver[2].strftime(COMPACT_DT_FMT)) or ver[3].strftime(COMPACT_DT_FMT)
    day_end = ver[2] and ver[2].strftime(COMPACT_DT_FMT) or datetime.today().strftime(COMPACT_DT_FMT)
    rt.update(on=[15,[[day_begin, day_end]]])
    status_ids = [cached_status[0] for cached_status in get_cache(7)]
    rt.update(of=[7,status_ids])
    return rt

def param_str(ky, vals):
    """Build param values for iss_call SQL function.
    >>> param_str(4, [28])
    "project_id:='=28'"
    >>> param_str(4, [[2, 5]])
    "project_id:='BETWEEN 2 AND 5'"
    >>> param_str(4, [2, 5])
    "project_id:='IN (2, 5)'"
    >>> param_str(2, [['130813', '130928']])
    "created_on:='BETWEEN ''130813'' AND ''130928'''"
    >>> param_str(2, ['130813', '130928'])
    "created_on:='IN (''130813'', ''130928'')'"
    """
    dim = _find_dim(ky)
    valfn = (dim[5]==1 and (lambda x:"''%s''"%x) or str )
    valstr = (lambda:
                ((_iseq(vals[0]) and ("BETWEEN %s AND %s" % tuple([valfn(val) for val in vals[0]]))) or 
                (len(vals)>1 and "IN (%s)" % ', '.join([valfn(val) for val in vals])) or 
                "=%s" % valfn(vals[0])))
    return "%s:='%s'" % (dim[3], valstr())
def recall_sql(qdict):
    """Build the SQL "select" sentence for the iss_recall DB function

    >>> recall_sql({"on":[4,[1,3,2]]})
    "SELECT * FROM iss_recall(project_id:='IN (1, 3, 2)')"
    >>> sql = recall_sql({"on":[4,[1,3,2]], "if":[[2,[['130813', '130928']]], [6,[3]]]})
    >>> sql.find("created_on:='BETWEEN ''130813'' AND ''130928'''") > 0
    True
    >>> sql.find("tracker_id:='=3'") > 0
    True
    >>> sql.find("project_id:='IN (1, 3, 2)'") > 0
    True
    >>> recall_sql({"if":[[15,['130926', '130928']]]})
    "SELECT * FROM iss_recall(theday:='IN (''130926'', ''130928'')', closest:='130928', days:=3)"
    >>> recall_sql({"on":[15,[['130926', '130928']]]})
    "SELECT * FROM iss_recall(theday:='BETWEEN ''130926'' AND ''130928''', closest:='130928', days:=3)"
    """
    def normalfn(vals): # rule 1: "ranged" values over "individual" values
        pair = [val for val in vals if _iseq(val)]
        return pair or vals
    params = copy.deepcopy(dict(qdict.get('if', {})))
    onpart = qdict.get('on', None)
    if onpart: params.update([onpart]) # rule 2: "on" part over "if" part
    params = dict([(ky, normalfn(vals)) for ky,vals in params.items()])
    closest, days, closest_param, days_param = None,None,None,None
    if pick_one(lambda x:x==15, params): #"day" is specified
        if _iseq(params[15][0]):
            closest, oldest = max(params[15][0]), min(params[15][0])
        else:
            closest, oldest = max(params[15]), min(params[15])
        if closest!=oldest:
            days = (datetime.strptime(closest, COMPACT_DT_FMT) - datetime.strptime(oldest, COMPACT_DT_FMT)).days +1
    param_strs = [param_str(*k_vals) for k_vals in params.items()]
    if closest: param_strs.append("closest:='%s'" % closest)
    if days: 
        if days > RECALL_DAYS:raise Exception(u'Recall %s days issues is too many' % days)
        param_strs.append("days:=%s" % days)
    return "SELECT * FROM iss_recall(%s)" % ', '.join(param_strs)

def _build_cat(qdict):
    dim_id, cvals = qdict['on']
    dim_def = _find_dim(dim_id)
    return dim_def, _scales(dim_def, cvals)

def _fact_fn():
    '''How to collect the fact data?'''
    return lambda x: x+1 #For now, we only sum on the number of issues. 

def _accum(lst, iss):
    cdim_def = lst[0][0]
    if len(lst[0]) > 2: #serials specified
        sdim_def, svals = lst[0][2:]
        iss_sv = iss[sdim_def[3]]
        sidx = [idx for idx, val in enumerate(svals) if val==iss_sv]
        if not sidx:return lst
        ser = lst[sidx[0]+1] # Find the serial in serial list. And serials is exclusive to each other when doing statisic
    else:
        ser = lst[1:]
    for cell in ser:# but accumulate on categories is not
        if _iscat(iss, cell[0], cdim_def):
            cell[1] = _fact_fn()(cell[1])
    return lst

def build_jqplot(q_dict, db_fn):#todo: 1. refactor the returned data structure for jqplot usage. 2. test the "if" part of the q_dict works.
    '''Build a datastructure for jqplot like:
        [{  cht:"bd|line|pie|...",
            flt:[ [dim1id,dim1nm,[(dv11, dv11str), (dv12, dv12str),...]],
                  [dim2id,dim2nm,[(dv21, dv21str), (dv22, dv22str),...]],
                  ... ],
            cat:[cid, cidstr, [(cv1, cv1str), (cv2, cv2str), ...]], 
            ser:[sid, sidstr, [(sv1, sv1str), (sv2, sv2str), ...]]},
         (s1c1v, s1c2v, ...),
         (s2c1v, s2c2v, ...),
         ...
        ]

    >>> dbfn = lambda x,y: [ # Mock database access function. this way the "if" part of the "q_dict" in all the following test will be ignored.
    ... {'author_id': 1, 'tracker_id':2, 'fixed_version_id':4, 'priority_id':1, 'updated_on':'130815'},
    ... {'author_id': 2, 'tracker_id':3, 'fixed_version_id':3, 'priority_id':2, 'updated_on':'130813'},
    ... {'author_id': 3, 'tracker_id':1, 'fixed_version_id':2, 'priority_id':2, 'updated_on':'130813'},
    ... {'author_id': 1, 'tracker_id':1, 'fixed_version_id':1, 'priority_id':1, 'updated_on':'130814'},
    ... {'author_id': 4, 'tracker_id':5, 'fixed_version_id':2, 'priority_id':3, 'updated_on':'130816'},]
    >>> q_dict = {"c":"line",
    ...         "on":[1,[1,3,2]], #category: author
    ...         "if":[[6,[1]], [10, [1,[2,5]]]],#tracker and version
    ...         "of":[8, [2,1,3]]} #serail: priority
    >>> plot = build_jqplot(q_dict, dbfn)
    >>> plot[0]['cht], plot[0]['ser']
    ()
    >>> plot[1:]
    [(2, 0, 0), (0, 1, 1), (0, 0, 0)]
    >>> q_dict = {"c":"line",
    ...         "on":[1,[1,3,2]], #category: author
    ...         "if":[[6,[1]], [10, [1,[2,5]]]]} #tracker and version
    >>> build_jqplot(q_dict, dbfn)
    [[1, [1, 2, 3]], (2, 1, 1)]
    >>> q_dict = {"c":"line", # two test: 1. range OVER list values(use "on"), 2. sum is correct
    ...         "on":[3,["120513",["130813","130815"]]], #category: updated_on. the individual value "120513" will be ignored
    ...         "if":[[6,[1]]],# tracker
    ...         "of":[8, [2,1,3]]} #serail: priority
    >>> plot = build_jqplot(q_dict, dbfn)
    >>> plot[0]
    [3, ['130813', '130814', '130815'], 8, [1, 2, 3]]
    >>> plot[1:]
    [(0, 1, 1), (2, 0, 0), (0, 0, 0)]
    '''
    is_burndown = q_dict['c'] in ('bd', 'burndown')
    if is_burndown:
        q_dict = justify4bd(q_dict)
    cdim, cvals = _build_cat(q_dict)
    of_part = q_dict.get('of', [])
    if of_part:
        sdim, svals = _find_dim(of_part[0]), of_part[1]
        svals = _scales(sdim, svals)
        if is_burndown: #For burndown chart, make "closed" and "rejected" stay on top.
            svals = [5,6] + [stid for stid in svals if stid not in [5,6]] 
        head = [cdim, cvals, sdim, svals]#Use a head row as helper information in the datastructure.
        rt_init = [head] + [[[cval, 0] for cval in cvals] for sval in svals]
    else:
        rt_init = [[cdim, cvals]] + [[cval, 0] for cval in cvals]
    _handler = lambda cur:[dict(map(lambda x,y:(x.name, y), cur.description, row)) for row in cur]
    issues = db_fn(recall_sql(q_dict), _handler)
    rt = reduce(_accum, issues, rt_init)
    if of_part:
        rt_head = [rt[0][0][0], rt[0][1], rt[0][2][0], rt[0][3]]
        rt_data = [zip(*svals)[1] for svals in rt[1:]]
    else:
        rt_head = [rt[0][0][0], rt[0][1]]
        rt_data = [zip(*rt[1:])[1]]
    return [rt_head] + rt_data

def _reform_val(vp):
    dim_name = _find_dim(vp[0])[1]
    return '%s: %s' % (dim_name,', '.join([_str_dimv(v) for v in vp[1]]))

def reform_cmd(q_dict):
    if_part = ' '.join([_reform_val(ele) for ele in q_dict.get('if',[])])
    input_on = q_dict.get('on',None)
    input_of = q_dict.get('of',None)
    on_part = input_on and _reform_val(input_on)
    of_part = input_of and _reform_val(input_of)
    k_part = lambda kw,vstr:vstr and kw+' ' + vstr
    kw_parts = [k_part(k,v) for k,v in [('of', of_part), ('on', on_part), ('if', if_part)]]
    return q_dict['c'] + ' ' + '\n    '.join([kwp for kwp in kw_parts if kwp])

def _build_tpl(q):
    tpl_vars = copy.deepcopy(global_tpl_vars)
    tpl_vars.update(jqdata = '""', err='')
    if not q:
        tpl_vars.update(cmd_txt='')
        return tpl_vars
    q_dict = json.loads(q)
    tpl_vars.update(cmd_txt = reform_cmd(q_dict))
    try:
        tpl_vars.update(jqdata = json.dumps(build_jqplot(q_dict, q_db)))
    except Exception as err:
        traceback.print_exc()
        tpl_vars.update(err=str(err))
    return tpl_vars

@get('/enum/<pid>/<did>')
def get_dim_enum(did, pid=0):
    dim_id, prj_id = int(did), int(pid)
    if dim_id not in [dim[0] for dim in DIM_LST if dim[4]]:
        return HTTPResponse('Dimension not found', status = 400)
    response.content_type = 'application/json; charset=UTF8'
    return json_enum(dim_id, prj_id)

@get('/')
@view('index')
def hm():
    '''Request Homepage, with optional query params.'''
    synfns = [(ddef[0],ddef[9]) for ddef in DIM_LST if ddef[9]]
    for dimid, synfn in synfns:
        cached = gcache.get(dimid, None)
        if (not cached) or (time_elapsed(cached[1]) > ENUM_CACHE_EXPIRE):
            eval(synfn)()
    return _build_tpl(request.query.decode().get('q',0))

# Static Routes
@get('/<filename:re:.*\.js>')
def javascripts(filename):
    return static_file(filename, root='static/js')

@get('/<filename:re:.*\.css>')
def stylesheets(filename):
    return static_file(filename, root='static/css')

if __name__=="__main__":
    lhost, lport = map(lambda x,y:x or y, sys.argv[1:],['127.0.0.1','9200'])
    run(host=lhost,port=lport,server='paste')