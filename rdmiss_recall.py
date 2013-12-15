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
#                 isid              integer,  
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
#                 root_id           integer 
#                )
# AS $fn$
from datetime import datetime, timedelta

params =(("to_char(created_on, 'YYYYMMDD')", "<='%s'" % closest.replace('-','')), #closest is a string like '2013-08-13'
         ('tracker_id',       tracker_id),
         ('project_id',       project_id),
         ('category_id',      category_id),
         ('status_id',        status_id), 
         ('assigned_to_id',   assigned_to_id),
         ('priority_id',      priority_id),
         ('fixed_version_id', fixed_version_id),
         ('author_id',        author_id))
closest_d = datetime.strptime(closest,'%Y-%m-%d')

def sql_where(params):
    pfn = lambda fnm, pstr: [fnm+ele.strip() for ele in pstr.split(',')]
    return ' AND '.join(sum([pfn(f,v) for f,v in params if v],[]))

day_issues = plpy.execute("SELECT * FROM issues WHERE %s" % sql_where(params)) #very return row is a dict

def recall_state(iss,recall_date): #todo
    return 1

for x in range(days):
    dt = closest_d - datetime.timedelta(days=x)
    day_issues = [recall_state(iss, dt) for iss in day_issues]
    for iss in day_issues:
        yield [dt] + list(map(lambda x:iss.values(), #go here

            ['id', 'tracker_id', 'project_id', 'due_date', 'category_id', 'status_id', 'assigned_to_id', 'priority_id', 'fixed_version_id', 'author_id', 'lock_version', 'created_on', 'updated_on', 'start_date', 'done_ratio', 'estimated_hours', 'parent_id', 'root_id']))
return


#  id               | integer                     | not null default nextval('issues_id_seq'::regclass) | plain    | 
#  tracker_id       | integer                     | not null default 0                                  | plain    | 
#  project_id       | integer                     | not null default 0                                  | plain    | 
#  subject          | character varying(255)      | not null default ''::character varying              | extended | 
#  description      | text                        |                                                     | extended | 
#  due_date         | date                        |                                                     | plain    | 
#  category_id      | integer                     |                                                     | plain    | 
#  status_id        | integer                     | not null default 0                                  | plain    | 
#  assigned_to_id   | integer                     |                                                     | plain    | 
#  priority_id      | integer                     | not null default 0                                  | plain    | 
#  fixed_version_id | integer                     |                                                     | plain    | 
#  author_id        | integer                     | not null default 0                                  | plain    | 
#  lock_version     | integer                     | not null default 0                                  | plain    | 
#  created_on       | timestamp without time zone |                                                     | plain    | 
#  updated_on       | timestamp without time zone |                                                     | plain    | 
#  start_date       | date                        |                                                     | plain    | 
#  done_ratio       | integer                     | not null default 0                                  | plain    | 
#  estimated_hours  | double precision            |                                                     | plain    | 
#  parent_id        | integer                     |                                                     | plain    | 
#  root_id          | integer                     |                                                     | plain    | 
#  lft              | integer                     |                                                     | plain    | 
#  rgt              | integer           

# $fn$ LANGUAGE plpython2u;