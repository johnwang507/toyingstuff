# CREATE OR REPLACE FUNCTION day_serial(upto date default current_date, days int default 31) RETURNS table(theday date) AS $$
from datetime import datetime, timedelta
return [datetime.strptime(upto,'%Y-%m-%d') - timedelta(days=i) for i in range(days)]
# $$ LANGUAGE plpython2u;


# CREATE OR REPLACE FUNCTION iss_recall(closest date default current_date, days int default 31, 
#                                          tracker_id text default null,
#                                          project_id text default null,
#                                          category_id text default null,
#                                          status_id text default null,
#                                          assigned_to_id text default null,
#                                          priority_id text default null,
#                                          fixed_version_id default null,
#                                          author_id default null)
# RETURNS TABLE ( theday            date,
#                 id                integer,  
#                 tracker_id        integer,  
#                 project_id        integer,    
#                 due_date          date,     
#                 category_id       integer,  
#                 status_id         integer,  
#                 assigned_to_id    integer,  
#                 priority_id       integer,  
#                 fixed_version_id  integer,  
#                 author_id         integer,  
#                 lock_version      integer,  
#                 created_on        timestamp,
#                 updated_on        timestamp,
#                 start_date        date,
#                 done_ratio        integer,  
#                 estimated_hours   double precision,
#                 parent_id         integer,  
#                 root_id           integer,
#                 closed_on         timestamp 
#                )
# AS $fn$
from datetime import datetime, timedelta
import copy

params =(("to_char(created_on, 'YYYYMMDD')", "<='%s'" % closest.replace('-','')), #closest is a string like '2013-08-13'
         ('is_private',       "is False"),
         ('tracker_id',       tracker_id),
         ('project_id',       project_id),
         ('category_id',      category_id),
         ('status_id',        status_id), 
         ('assigned_to_id',   assigned_to_id),
         ('priority_id',      priority_id),
         ('fixed_version_id', fixed_version_id),
         ('author_id',        author_id))
closest_dt = datetime.strptime(closest,'%Y-%m-%d')
journal_sql="""SELECT jn.id jid, jn.created_on crn, 
                      jnd.prop_key prp, jnd.old_value ov, jnd.value nv
               FROM   journals jn, journal_details jnd
               WHERE  jn.id = jnd.journal_id AND
                      jn.journalized_type = 'Issue' AND
                      jnd.property = 'attr' AND
                      jn.journalized_id = $1 AND
                      jn.created_on > $2
               ORDER BY crn desc """
journal_q = plpy.prepare(journal_sql, ['int', 'timestamp'])

def sql_where(params):
    pfn = lambda fnm, pstr: [fnm+ele.strip() for ele in pstr.split(',')]
    return ' AND '.join(sum([pfn(f,v) for f,v in params if v],[]))

day_issues = plpy.execute("SELECT * FROM issues WHERE %s" % sql_where(params))

def recall_state(iss,to_datetime):
    todt = to_datetime.strftime('%Y-%m-%d') + ' 23:59:59'
    jnds = plpy.execute(journal_q, [iss['id'], todt])
    org_state = copy.deepcopy(iss)
    [org_state.update({row['prp']: row['ov']}) for row in jnds]
    return org_state    

def iss_vals(iss):
    return map(lambda x:iss.get(x,None),
                ['id', 'tracker_id', 'project_id', 'due_date', 'category_id',
                'status_id', 'assigned_to_id', 'priority_id', 'fixed_version_id', 
                'author_id', 'lock_version', 'created_on', 'updated_on', 'start_date',
                'done_ratio', 'estimated_hours', 'parent_id', 'root_id', 'closed_on'])

for x in range(days):
    dt = closest_dt - datetime.timedelta(days=x)
    day_issues = [iss for iss in day_issues if iss['created_on'][:10] <= dt.strftime('%Y-%m-%d')]
    day_issues = [recall_state(iss, dt) for iss in day_issues]
    for iss in day_issues:
        yield [dt] + iss_vals(iss)
# $fn$ LANGUAGE plpython2u;