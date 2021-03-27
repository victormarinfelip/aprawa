import pymysql
from configparser import ConfigParser
from sqlalchemy import create_engine
from typing import Tuple, List


class MySqlConnector(object):

    def __init__(self, env: str = None, db_ini_path: str = "data/mysql_config.ini"):
        config = ConfigParser()
        config.read(db_ini_path)

        self.host = config.get(env, 'host')
        self.port = int(config.getint(env, 'port'))
        self.user = config.get(env, 'user')
        self.password = config.get(env, 'password')
        self.database_name = config.get(env, 'db_name')
        self._connector = pymysql.connect(host=self.host, port=self.port,
                                          user=self.user, password=self.password, db=self.database_name)

        uri = "mysql+pymysql://{}:{}@{}:{}/{}".format(self.user, self.password,
                                                      self.host, 3306, self.database_name)

        # We're using also a SQLAlchemy engine for the dataset upload
        self.engine = create_engine(uri, echo=True)

    def query(self, sql: str, params: List = None) -> Tuple[List, Tuple]:
        cursor = self._connector.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            headers = [column[0] for column in cursor.description]
            values = cursor.fetchall()
        finally:
            cursor.close()
        return headers, values

    def close(self):
        if self._connector:
            self._connector.close()
