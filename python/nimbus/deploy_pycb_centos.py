#!/usr/bin/env python
import subprocess,sqlite3

def call(command):
    subprocess.call('%s'%command, shell=True)

location='/opt/nimbus/ve/lib/python2.6/site-packages/pycb/'
conn=sqlite3.connect('%sbak/sqlite3.db'%location)
cur=conn.cursor()
try:
    cur.execute('select * from deploy_pycb order by id desc limit 0,1')
    result=cur.fetchall()
    print result
    try:
        backup_count=result[0][0]+1
    except IndexError:
        backup_count=1
except sqlite3.OperationalError:
    cur.execute('create table deploy_pycb (id integer primary key not null, time text not null default current_time, date text not null default current_date)')

call('cp %spycb-0.1-py2.6.egg %sbak/pycb-0.1-py2.6.egg_%s'%(location,location,backup_count))
cur.execute('insert into deploy_pycb (id) values (?)',(backup_count,))

conn.commit()
cur.close()

call("echo '123'|sudo -S cp -r /home/levi/Dropbox/workspace/pycb %s"%location)
call('echo "123"|sudo -S rm %spycb-0.1-py2.6.egg'%location)
call('zip -r pycb-0.1-py2.6.egg docs EGG-INFO pycb')
call('cp %spycb-0.1-py2.6.egg %s../'%(location,location))
call('/opt/nimbus/bin/nimbusctl cumulus restart')
