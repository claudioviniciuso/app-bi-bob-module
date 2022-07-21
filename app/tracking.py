from datetime import datetime as dtime
import string
import pytz
import random

UTC = pytz.utc
IST = pytz.timezone('Brazil/East')

class tracking_steps:
    def __init__(self,cnn, cursor,table):
        self.session_name = None
        self.cnn = cnn
        self.cursor = cursor
        self.table = table
        self.id_session = None
        

    def post_session(self):
        k1 = random.randrange(1,24)
        k2 = random.randrange(1,24)
        k3 = random.randrange(1,24)
        key = list(string.ascii_uppercase)[k1] + list(string.ascii_uppercase)[k2] + list(string.ascii_uppercase)[k3]

        self.session_name = key + str(dtime.now(IST).strftime("%d%m%Y%M%S%f"))

        qry = """Insert into log_app_bi.sessions (SESSION_NAME, CREATED_AT, "table") values ('%s', CURRENT_TIMESTAMP,'%s')"""
        self.cursor.execute(qry %(self.session_name, self.table))
        self.cnn.commit()

        self.cursor.execute("Select id from log_app_bi.sessions where SESSION_NAME = '%s'" %(self.session_name))
        self.id_session = self.cursor.fetchone()[0]

        pass

    def log_tracking_session(self, task_id, endpoint_,page_,start_datetime, end_datetime,result,qt_insert_rows, error_description):
        qry = """insert into log_app_bi.tracking_sessions values (%s,%s,'%s',%s,'%s','%s',%s,%s,'%s')"""
        self.cursor.execute(qry %(self.id_session, task_id, endpoint_,page_, start_datetime,end_datetime,result,qt_insert_rows,error_description))
        self.cnn.commit()

    
    def log_tracking_etl(self, endpoint_,start_datetime, end_datetime,new_rows, updated_rows, prev_rows, after_rows, deleted_rows):
        qry = """insert into log_app_bi.tracking_ETL values (%s,'%s','%s','%s',%s,%s,%s,%s,%s)"""
        self.cursor.execute(qry %(self.id_session, endpoint_, start_datetime,end_datetime,new_rows,updated_rows,prev_rows,after_rows,deleted_rows))
        self.cnn.commit()
    
    def log_tracking_insert(self, table_, rows_insert, rows_error):
        qry = "insert into log_app_bi.tracking_insert values ('%s', '%s', %s, %s)"
        self.cursor.execute(qry %(self.id_session,table_, rows_insert, rows_error))
        self.cnn.commit()