import logging

TUSHARE_TOKEN = 'b4a8abd43d74fcd22b2b811918e4f126488ba48a3d348189627247b6'
DB_CONN_STR = 'mysql://tushare:pwd123@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1'
DATE_FORMAT = '%Y%m%d'
LOG_LEVEL = logging.INFO

# func: times/min
THROTTLE_RATES = {
    'daily': 500,
    'adj_factor': 500
}