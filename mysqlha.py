#!/usr/bin/python
import os, sys
import time
import pymysql
import logging
import dns

master_server='mysql.db.com'
slave_server='mysql-readonly.db.com'

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %X',
    filename='mysqlha.log',
    filemode='a')


def sql_conn(server,stmt):
    mysqlconn=pymysql.connect(host=server,port=int(sys.argv[1]),user='root',passwd='123456',db='dbmanager')
    cursor=mysqlconn.cursor()
    cursor.execute(stmt)
    #cursor.execute("select sleep(0.1) ;")
    ds=cursor.fetchall()
    cursor.close()
    mysqlconn.commit()
    mysqlconn.close()
    return ds

def confirm_mas_pos():
    pos_sql="show master status;"
    old_mpos=sql_conn(master_server,pos_sql)
    logging.info(': old master file  %s  pos %s'%(old_mpos[0][0],str(old_mpos[0][1])))
    time.sleep(1)
    new_mpos=sql_conn(master_server,pos_sql)
    logging.info(': new master file  %s  pos %s'%(new_mpos[0][0],str(new_mpos[0][1])))
    #compare master status 
    if ( old_mpos[0][0]==new_mpos[0][0] and   old_mpos[0][1]==new_mpos[0][1] ):
        logging.info(': master status no change ')
        return 1
    else:
        return 0

def kill_process():
    ds=sql_conn(master_server,"""SELECT  CONCAT('kill ',id,';') FROM information_schema.PROCESSLIST WHERE  command='Sleep' """)
    try:
        for sql in ds[0]:
            sql_conn(master_server,sql)
            logging.info(': kill process %s'%(sql))
    except Exception as e:
            logging.info(': no active connections ')


def confirm_slave_no_delay():
    master_time=sql_conn(master_server,'SELECT c_currtime FROM  t_dba_timediff;')
    slave_time=sql_conn(slave_server,'SELECT c_currtime FROM  t_dba_timediff;')
    if (master_time != slave_time):
        return 0
    else:
        logging.info(': master time %s equal slave time %s no delay'%(master_time[0][0],slave_time[0][0]))
        return 1

def change_dns_resolve():
    pass

if __name__=='__main__':
    logging.info(': %s'%(20*'*'))
    # master set read only and  kill processlist
    sql_conn(master_server,' SET GLOBAL   super_read_only=ON ; SET GLOBAL  READ_ONLY=ON;')
    logging.info(': master set read_only=on ')
    kill_process()
    
    #confirm master status  no change and  slave no delay
    confirm_mas_pos()
    confirm_slave_no_delay()
    sql_conn(slave_server,'SET GLOBAL   super_read_only=off ;SET GLOBAL  READ_ONLY=off;')
