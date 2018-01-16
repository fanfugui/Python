#/usr/bin/python

import threading
from Queue import Queue
import MySQLdb
import sys
import time
import datetime



def mysqlconn(vsql, **conn_str):
    conn = MySQLdb.connect(host=conn_str['host'],
                           user=conn_str['user'],
                           passwd=conn_str['passwd'],
                           port=conn_str['port'],
                           db=conn_str['db'])
    cur = conn.cursor()
    cur.execute(vsql)
    ds = cur.fetchall()
    conn.commit()
    conn.close()
    return ds


def is_int(val):
    try:
        x = int(val)
        return True
    except ValueError:
        return False


def get_metrics(**conn_str):
    monitor_sql = " SHOW GLOBAL STATUS ;"
    ds = mysqlconn(monitor_sql, **conn_str)
    return ds


def get_checkpointage(**conn_str):
    monitor_sql = " SHOW ENGINE INNODB STATUS ;"
    ds = mysqlconn(monitor_sql, **conn_str)
    lsn = 0
    lcp = 0
    for line in ds[0][2].split('\n'):
        # get lsn
        if line.startswith('Log flushed'):
            nums = [num for num in line.split(' ') if is_int(num)]
            lsn = nums[0]
            # get last checkpoint at
        if line.startswith('Last checkpoint at'):
            nums = [num for num in line.split(' ') if is_int(num)]
            lcp = nums[0]
    ckp_age = int(lsn) - int(lcp)
    return ckp_age


def inset_all_metrics(**conn_str):
    ds_result = []
    ds = get_metrics(**conn_str)
    metrics = ['Com_update',
               'Com_select',
               'Com_insert',
               'Com_delete',
               'Questions',
               'Threads_running',
               'Threads_connected',
               'Open_tables',
               'Open_files',
               'Innodb_row_lock_current_waits',
               'Innodb_row_lock_time',
               'Innodb_row_lock_time_avg',
               'Innodb_row_lock_time_max',
               'Innodb_row_lock_waits',
               'Innodb_rows_deleted',
               'Innodb_rows_inserted',
               'Innodb_rows_read',
               'Innodb_rows_updated',
               'Innodb_buffer_pool_pages_free',
               'Innodb_buffer_pool_pages_data',
               'Innodb_buffer_pool_pages_dirty',
               'Innodb_buffer_pool_pages_total',
               'Innodb_buffer_pool_read_requests',
               'Innodb_buffer_pool_reads',
               'Bytes_received',
               'Bytes_sent']

    for i in ds:
        if i[0] in metrics:
            ds_result.append(i)

    cpa = ('Checkpointage', get_checkpointage(**conn_str))
    ds_result.append(cpa)
    conn = MySQLdb.connect(host=conn91_str['host'],
                           user=conn91_str['user'],
                           passwd=conn91_str['passwd'],
                           port=conn91_str['port'],
                           db=conn91_str['db'])
    cur = conn.cursor()
    sql = "insert into mysql_metrics_counter(ip ,port,item ,mVALUE ,createtime) values('" + conn_str['host'] + "','" + str(conn_str['port']) + "',%s,%s,'" + dt + "') "
    cur.executemany(sql, ds_result)
    conn.commit()
    conn.close()


def concurrent_monitor(q):
    while 1:
        if q.qsize() == 0:
            print "monitor end at %s" % (datetime.datetime.now())
            return
        else:
            list_conn = q.get()
            print list_conn['host'], list_conn['port']
            inset_all_metrics(**list_conn)


if __name__ == '__main__':
    print "monitor start at %s" % (datetime.datetime.now())
    dt = time.strftime("%Y-%m-%d %H:%M:00")
    conn91_str = {"host": "monitor.db.com", "user": "test", "passwd": "123456", "port": 3306, "db": "dbmonitor"}
    #get mysql ip lists
    list_sql = "SELECT serverip ,serverport FROM  dbmonitor.t_mysqlserverinfo WHERE ismonitor=1"
    que = Queue()
    for i in mysqlconn(list_sql, **conn91_str):
        list_conn = {"host": i[0], "user": "test", "passwd": "123456", "port": int(i[1]), "db": "dbmanager"}
        que.put(list_conn)

    l_threads=[]
    for j in range(5):
        t = threading.Thread(target=concurrent_monitor, args=(que,))
        t.start()
        l_threads.append(t)


    for t in l_threads:
        t.join()


    conn = MySQLdb.connect(host=conn91_str['host'],
                           user=conn91_str['user'],
                           passwd=conn91_str['passwd'],
                           port=conn91_str['port'],
                           db=conn91_str['db'])
    cur = conn.cursor()
    sql = """delete from mysql_metrics_counter where createtime < date_sub(now(), interval 60 day)"""
    cur.execute(sql,)
    conn.commit()
    conn.close()


#print "monitor end at %s" %(datetime.datetime.now())

