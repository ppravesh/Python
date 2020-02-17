import cx_Oracle
import os
import sys
from pprint import pprint
connection = cx_Oracle.Connection("%s/%s@%s" % ('system', '<passwd>>', '<SID>'))
##connection = cx_Oracle.Connection("%s/%s@%s" % (uname,passwd,conn))
cursor = cx_Oracle.Cursor(connection)
Baseline_Snaps="select 'Baseline Name <=> End Snap ID <=> End Snap time ' from dual \
        union all \
        select baseline_name||'<=>'||end_snap_id||'<=>'||end_snap_time  \
        from dba_hist_baseline"
cursor.execute(Baseline_Snaps)
pprint (cursor.fetchall())
begin_snap=input('Enter Begin Snap ID :')
end_snap=input('Enter 2nd Snap ID :')
BaseLineSqlIDs_tmp = "select  ss.sql_id,round((ss.elapsed_time_delta/1000)/decode(ss.executions_delta,0,1,ss.executions_delta),2),ss.executions_delta, \
ss.snap_id,to_char(S.END_INTERVAL_TIME,'DDMONYYYY_HH24MI') \
from    dba_hist_snapshot       s, \
        dba_hist_sqlstat        ss \
where   ss.dbid = s.dbid \
and     ss.instance_number = s.instance_number \
and     ss.snap_id = s.snap_id \
and   ss.executions_delta > 0 \
and ss.parsing_schema_name='WEBUSER' \
and     ss.snap_id = "
BaseLineSqlIDs=BaseLineSqlIDs_tmp+begin_snap

CurrentSqlIDs_tmp = "select  ss.sql_id, \
		round((ss.elapsed_time_delta/1000)/decode(ss.executions_delta,0,1,ss.executions_delta),2), \
		ss.executions_delta, \
		ss.rows_processed_delta, \
		round(ss.rows_processed_delta/decode(ss.executions_delta,0,1,ss.executions_delta),2), \
		round(ss.buffer_gets_delta/decode(ss.executions_delta,0,1,ss.executions_delta),2), \
		ss.elapsed_time_delta/1000 \
from    dba_hist_snapshot       s, \
        dba_hist_sqlstat        ss \
where   ss.dbid = s.dbid \
and     ss.instance_number = s.instance_number \
and     ss.snap_id = s.snap_id \
and   ss.executions_delta > 0 \
and ss.parsing_schema_name='WEBUSER' \
and     ss.snap_id = "
CurrentSqlIDs = CurrentSqlIDs_tmp+end_snap

base_sqlid=open('bsql_id.txt','w')
base_sqlid=open('bsql_id.txt','a')
cursor.execute(BaseLineSqlIDs)
base_data=cursor.fetchall()
for base_record in base_data:
	baseline_sqlid=base_record[0]
	baseline_elapse_time=base_record[1]
	baseline_executions=base_record[2]
	baseline_snapid=base_record[3]
	baseline_snapInterval=base_record[4]
	#make_data=base_record[0]+' '+str(base_record[1])+' '+str(base_record[2])+' '+str(base_record[3])+' '+str(base_record[4])
	make_data=baseline_sqlid+'\t'+str(baseline_elapse_time)+'\t'+str(baseline_executions)+'\t'+str(baseline_snapid)+'\t'+str(baseline_snapInterval)
	base_sqlid.write(make_data)
	base_sqlid.write('\n')
base_sqlid.close()
bdict = {}
base_sqlid=open('bsql_id.txt')
for line in base_sqlid:
       (sqlid, elt,exe,snapid,snaptime) = line.split()  ## .split is to convert all columns of that row to list
       #print (line.split())
       bdict[(sqlid)] = float(elt)   #### Prepare dictionary
print ('BDICT')
print(bdict)
cursor.execute(CurrentSqlIDs)
data = cursor.fetchall()
new_sqlid=open('New_sqlid.txt','w')
new_sqlid=open('New_sqlid.txt','a')
dgrd_sqlid=open('Degraded_sqlid.txt','w')
dgrd_sqlid=open('Degraded_sqlid.txt','a')
imprvd_sqlid=open('Improved_sqlid.txt','w')
imprvd_sqlid=open('Improved_sqlid.txt','a')
NoChange_sqlid=open('NoChange_sqlid.txt','w')
NoChange_sqlid=open('NoChange_sqlid.txt','a')
for x in data:
	current_sid=(x[0])
	current_sid_time=(x[1])
	executions=(x[2])
	rows_processed=(x[3])
	rows_per_exec=(x[4])
	GetsPerExec=(x[5])
	ElapseTimeMS=(x[6])
	if current_sid not in bdict:
		a1 = 'NewSqlID : '+current_sid+' El_time/Exe: '+str(current_sid_time)+', Executions : '+str(executions)+', GetsPerExec :'+str(GetsPerExec)+\
			',RowperExec: =ROUND('+str(rows_processed)+'/'+str(executions)+',2)'
		new_sqlid.write(a1)
		new_sqlid.write('\n')
	else:
		base_sid_time=bdict[current_sid]
		if base_sid_time>float(current_sid_time):
			pct_impr = round(((base_sid_time-float(current_sid_time))/float(current_sid_time))*100,2)
			b1 = 'ImprvdSqlID: '+current_sid+' BaselineEL/Exec: '+str(base_sid_time)+' CurrentEl/Exec: '+str(current_sid_time)+\
				' Imprvd%: '+str(pct_impr)+',Executions: '+str(executions)+\
				', GetsPerExec :'+str(GetsPerExec)+\
				',RowperExec: =ROUND('+str(rows_processed)+'/'+str(executions)+',2)'
			imprvd_sqlid.write(b1)
			imprvd_sqlid.write('\n')
		elif base_sid_time<float(current_sid_time):
			pct_impr = round(((float(current_sid_time)-base_sid_time)/base_sid_time)*100,2)
			c1 = 'DgrdSqlID: '+current_sid+' BaselineEL/Exec: '+str(base_sid_time)+' CurrentEL/Exec: '+str(current_sid_time)+\
				' Dgrd%: '+str(pct_impr)+',Executions: '+str(executions)+\
                                ', GetsPerExec :'+str(GetsPerExec)+\
				',RowperExec: =ROUND('+str(rows_processed)+'/'+str(executions)+',2)'
			dgrd_sqlid.write(c1)
			dgrd_sqlid.write('\n')
		else:
			d1 = 'No Change SQLIDs: '+current_sid+' BaselineEL/Exec: '+str(base_sid_time)+' CurrentEL/Exec: '+str(current_sid_time)+\
                                ',Executions: '+str(executions)+\
                                ', GetsPerExec :'+str(GetsPerExec)+\
                                ',RowperExec: =ROUND('+str(rows_processed)+'/'+str(executions)+',2)'
			NoChange_sqlid.write(d1)
			NoChange_sqlid.write('\n')
new_sqlid.close()
imprvd_sqlid.close()
imprvd_sqlid=open('Improved_sqlid.txt','a')
NoChange_sqlid=open('NoChange_sqlid.txt','w')
NoChange_sqlid=open('NoChange_sqlid.txt','a')
dgrd_sqlid.close()
NoChange_sqlid.close()
cursor.close()
connection.close()
print ('New SQL IDs')
print ('================')
NewSQLID=open('New_sqlid.txt','r')
for x in NewSQLID:
	x = x.strip()
	print (x)
print ('\n')
print ('Degraded SQL IDs')
print ('================')

DegSQLID=open('Degraded_sqlid.txt','r')
for y in DegSQLID:
	y = y.strip()
	print (y)
print ('Improved SQL IDs')
print ('================')

ImprvdSQLID=open('Improved_sqlid.txt','r')
for z in ImprvdSQLID:
	z = z.strip()
	print (z)
print ('NoChange SQL IDs')
print ('================')
NoChangeSQLID=open('NoChange_sqlid.txt','r')
for z1 in NoChangeSQLID:
	z1 = z1.strip()
	print (z1)
