import pymysql
from sqlalchemy import create_engine
import config

pymysql.install_as_MySQLdb()
engine_ts = create_engine(config.DB_CONN_STR, echo=False) # pool_size=3, max_overflow=0