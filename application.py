from app import connect_db
from app import request_api
from app import stats
from datetime import date, datetime, timedelta
import time
import logging
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler


if __name__ == 'application':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
        filename='app/logger.log',
        filemode='a'
        )

app_log = logging.getLogger(__name__)
 
def execute(cnn, cursor, updated_at_min,stats_db):
    
    #Requisição Token
    cursor.execute("Select * from secrets.token_shopify")
    access_token = cursor.fetchone()[0]

    #Validar Data
    if updated_at_min == 'today':
        updated_at_min = date.today()
        fl_valid_date = True
    else:
        try:
            fl_valid_date = bool(datetime.strptime(updated_at_min, "%Y-%m-%d"))
        except:
            fl_valid_date = False
            pass

    #Stats API
    app_stats = stats.data_stats(access_token,cnn,cursor)
    status_api = app_stats.stats_api()
    
    if fl_valid_date == False:
        print('Invalid updated_at_min: %s' %(updated_at_min))
    elif status_api['data'][0]['status_code'] != 200:
        print('API Shopify OFF')
    elif stats_db['status_connection'] == False:
        print('Failed to connect to server')
    else:
        print('Iniciado em: %s' %(datetime.now()))
        app_request = request_api.request_api(cnn, cursor, token=access_token, table='all')
        app_request.request_collects(updated_at_min)
        app_request.request_products(updated_at_min)
        app_request.request_customers(updated_at_min)
        app_request.request_orders(updated_at_min)
        print('Finalizado em: %s' %(datetime.now()))   

    pass

def check_time():
    
    # Conexão com Banco 
    connect_to = connect_db.config_db()
    stats_db = connect_to.exec() 
    cnn = connect_to.cnn
    cursor = cnn.cursor()

    #Resgatar Default Time
    cursor.execute("Select config_value from log_app_bi.config where id_config = 2")
    scheduled_time = cursor.fetchone()[0]
    scheduled_time = datetime.strptime(str(date.today()) + ' ' + scheduled_time, '%Y-%m-%d %H:%M:%S')
    print('Scheduled Time: %s' %(scheduled_time))

    actual_datetime = datetime.now()

    if actual_datetime > scheduled_time:
        scheduled_time = scheduled_time + timedelta(days=1)

    default_timesleep = (scheduled_time - actual_datetime).total_seconds() - 60

    print('Time Sleep: %s' %(default_timesleep))

    cnn.close()

    return default_timesleep

def run_app():
    # Conexão com Banco 
    connect_to = connect_db.config_db()
    stats_db = connect_to.exec() 
    cnn = connect_to.cnn
    cursor = cnn.cursor()

    #Resgatar Update_at_min_Config
    cursor.execute("Select config_value from log_app_bi.config where id_config = 1")
    updated_at_min = cursor.fetchone()[0]
    print('Updated_at_min: %s' %(updated_at_min))

    #Resgatar Update_at_min_Config
    cursor.execute("Select config_value from log_app_bi.config where id_config = 3")
    status_app = cursor.fetchone()[0]
    print('Status APP: %s' %(status_app))


    if status_app == '1':
        execute(cnn,cursor,updated_at_min,stats_db)
    
    cnn.close()


scheduler = BlockingScheduler()
print('Agendado')
job_run = scheduler.add_job(run_app, 'interval', minutes=10)
scheduler.start()

