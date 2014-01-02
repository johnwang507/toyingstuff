# CREATE OR REPLACE FUNCTION day_serial(upto date default current_date, days int default 31) RETURNS table(theday date) AS $$
from datetime import datetime, timedelta
return [datetime.strptime(upto,'%Y-%m-%d') - timedelta(days=i) for i in range(days)]
# $$ LANGUAGE plpython2u;

# CREATE OR REPLACE FUNCTION iss_recall(closest date default current_date, days int default 1,
#                                          theday text default null,
#                                          tracker_id text default null,
#                                          project_id text default null,
#                                          category_id text default null,
#                                          status_id text default null,
#                                          assigned_to_id text default null,
#                                          priority_id text default null,
#                                          fixed_version_id text default null,
#                                          author_id text default null,
#                                          done_ratio text default null,
#                                          due_date text default null,        
#                                          created_on text default null,      
#                                          updated_on text default null,      
#                                          start_date text default null,      
#                                          estimated_hours text default null)
# RETURNS TABLE ( theday            text,
#                 id                integer,  
#                 tracker_id        integer,  
#                 project_id        integer,    
#                 due_date          text,     
#                 category_id       integer,  
#                 status_id         integer,  
#                 assigned_to_id    integer,  
#                 priority_id       integer,  
#                 fixed_version_id  integer,  
#                 author_id         integer,  
#                 lock_version      integer,  
#                 created_on        text,
#                 updated_on        text,
#                 start_date        text,
#                 done_ratio        integer,  
#                 estimated_hours   double precision,
#                 parent_id         integer,  
#                 root_id           integer,
#                 closed_on         text 
#                )
# AS $fn$
from datetime import datetime, timedelta
import copy

closest_dt = datetime.strptime(closest,'%Y-%m-%d') # PlPython passed in date/datetime as String in this format  
date2str = lambda x:"to_char(%s, 'YYMMDD') %s" % (x, x)

where_cols =(("to_char(created_on, 'YYMMDD')", "<='%s'" % closest.replace('-','')[2:]), #closest is a string like '2013-08-13'
            ('is_private',                          "is False"),
            ('tracker_id',                          tracker_id),
            ('project_id',                          project_id),
            ('category_id',                         category_id),
            ('status_id',                           status_id), 
            ('assigned_to_id',                      assigned_to_id),
            ('priority_id',                         priority_id),
            ('fixed_version_id',                    fixed_version_id),
            ('author_id',                           author_id),
            ('done_ratio',                          done_ratio),
            ("to_char(due_date,   'YYMMDD')",       due_date), # The passed in date/datetime param must be like '120813'
            ("to_char(created_on, 'YYMMDD')",       created_on),
            ("to_char(updated_on, 'YYMMDD')",       updated_on),
            ("to_char(start_date, 'YYMMDD')",       start_date),
            ('estimated_hours',                     estimated_hours))

selected_cols = (('id',                   str),
                 ('tracker_id',           str),
                 ('project_id',           str),
                 ('due_date',             date2str),
                 ('category_id',          str),
                 ('status_id',            str),
                 ('assigned_to_id',       str),
                 ('priority_id',          str),
                 ('fixed_version_id',     str),
                 ('author_id',            str),
                 ('lock_version',         str),
                 ('created_on',           date2str),
                 ('updated_on',           date2str),
                 ('start_date',           date2str),
                 ('done_ratio',           str),
                 ('estimated_hours',      str),
                 ('parent_id',            str),
                 ('root_id',              str),
                 ('closed_on',            date2str))
journal_sql="""SELECT jn.id jid, jn.created_on crn, 
                      jnd.prop_key prp, jnd.old_value ov, jnd.value nv
               FROM   journals jn, journal_details jnd
               WHERE  jn.id = jnd.journal_id AND
                      jn.journalized_type = 'Issue' AND
                      jnd.property = 'attr' AND
                      jn.journalized_id = $1 AND
                      to_char(jn.created_on, 'YYMMDD') > $2
               ORDER BY crn desc """
journal_q = plpy.prepare(journal_sql, ['int', 'text'])

def sel_cols():
    return ', '.join([sfn(col) for col, sfn in selected_cols])
def sql_where():
    return ' AND '.join('%s %s' % (f, v) for f, v in where_cols if v)
day_issues = plpy.execute("SELECT %s FROM issues WHERE %s" % (sel_cols(), sql_where()))

def iss_vals(iss):# To ensure the right order of returned columns.
    return map(lambda x:iss.get(x,None), zip(*selected_cols)[0])

def recall_state(iss,ymd):
    jnds = plpy.execute(journal_q, [iss['id'], ymd])
    org_state = copy.deepcopy(iss)
    [org_state.update({row['prp']: row['ov']}) for row in jnds]# Be careful on multiple different value types for a column after this!
    if jnds: org_state.update(updated_on=(jnds[0]['crn'][2:10]).replace('-',''))
    return org_state    

def iselected(ymd):
    if not theday:return True
    if theday.startswith('BETWEEN '):
        return (ymd >= theday[9:15]) and (ymd <= theday[22:28])
    if theday.startswith('IN '):
        return ymd in [d[1:-1] for d in theday[4:-1].split(', ')]
    if theday.startswith('='):
        return ymd == theday[1:]

for x in range(days):
    ymd = (closest_dt - timedelta(days=x)).strftime('%y%m%d')
    day_issues = [iss for iss in day_issues if iss['created_on'] <= ymd]
    day_issues = [recall_state(iss, ymd) for iss in day_issues]
    for iss in day_issues:
        if iselected(ymd):
            yield [ymd] + iss_vals(iss)
# $fn$ LANGUAGE plpython2u;