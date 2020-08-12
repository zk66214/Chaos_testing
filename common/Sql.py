# -*- coding:utf-8 -*-

from config_utils import parse_config
from common import Log
import jaydebeapi


class Init:
    def __init__(self, test_env='P1-K8S-ENV', db_type='db'):
        self.__test_env = test_env
        self.__config = parse_config.ConfigUtil(self.__test_env)
        self.__log_config = parse_config.ConfigUtil("LOG")
        self.__driver = self.__config.get_driver()

        if db_type != 'db':
            self.__url = self.__config.get_meta_db_jdbc_url()
        else:
            self.__url = self.__config.get_db_jdbc_url()

        self.__user = self.__config.get_db_user()
        self.__password = self.__config.get_db_pwd()
        self.__jar = self.__config.get_ldb_driver_file()

        self.__conn = jaydebeapi.connect(self.__driver, self.__url, [self.__user, self.__password], self.__jar, )
        self.__curs = self.__conn.cursor()

        self.__log = Log.MyLog()

    """
    执行sql，若有返回值，return表信息，若无返回值，return None
    :return: None
    """

    def run(self, sql=''):
        self.__log.info("执行SQL：{0}".format(sql))

        try:
            self.__curs.execute(sql)
            self.__log.info('SQL执行成功')

        except Exception as e:
            self.__log.error('SQL执行失败!异常信息：{0}'.format(str(e)))
            return str(e)

        try:
            rows = self.__curs.fetchall()

            if rows:
                if str(self.__log_config.get_log_level()).upper() == "DEBUG":
                    self.__log.info('SQL返回值:')
                    for row in rows:
                        self.__log.info(str(row))
                return rows
            self.__log.info('SQL无返回值')
            return 'No Response'

        except Exception as e:
            self.__log.info('SQL无返回值')
            return 'No Response'

    """
    释放jdbc连接
    """
    def disconnect(self):
       self.__curs.close()
       self.__conn.close()