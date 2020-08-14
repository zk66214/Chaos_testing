# -*- coding:utf-8 -*-

import configparser
import glob
import os

class ConfigUtil:
    def __init__(self, envInfo="P1-K8S-ENV"):
        self.__path = os.getcwd()
        self.__iniFilePath = glob.glob(os.path.abspath(os.path.dirname(__file__)).split('config_utils')[0] + "conf/" + "*.ini")
        self.__driverFilePath = os.path.abspath(os.path.dirname(__file__)).split('config_utils')[0] + "driver/"
        self.__envInfo = envInfo

    def __read_config(self, feild, key):
        """
        Features：内部函数根据section，key，返回配置项value

        :param feild: section值
        :param key: section下面的key
        :return: 返回value
        """
        cfg = configparser.ConfigParser()
        cfg.read(self.__iniFilePath, encoding='utf-8')

        try:
            result = cfg.get(feild, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            print("Non - existent section or non - existing options")
        try:
            return result
        except UnboundLocalError:
            print(u"The result of reference to a local variable before assignment")

    def get_config(self, feild, key):
        """
        Features: 获取配置项

        :return: 返回value
        """
        return self.__read_config(feild, key)


    def get_test_user(self):
        """
        Features: 获取服务器测试账号

        :return: test_user
        """
        return self.__read_config(self.__envInfo, "test_user")

    def get_test_pwd(self):
        """
        Features: 获取服务器测试账号密码

        :return: test_pwd
        """
        return self.__read_config(self.__envInfo, "test_pwd")

    def get_authority_user(self):
        """
        Features: 获取服务器高权限账号

        :return: authority_user
        """
        return self.__read_config(self.__envInfo, "authority_user")

    def get_authority_pwd(self):
        """
        Features: 获取服务器高权限账号的密码

        :return: authority_pwd
        """
        return self.__read_config(self.__envInfo, "authority_pwd")

    def get_host_ips(self):
        """
        Features: 获取服务器ip列表

        :return: host_ips
        """
        return self.__read_config(self.__envInfo, "host_ips")

    def get_host_names(self):
        """
        Features: 获取服务器name列表

        :return: host_names
        """
        return self.__read_config(self.__envInfo, "host_names")

    def get_main_server_ip(self):
        """
        Features: 获取DB部署的机器ip

        :return: mian_server_ip
        """
        return self.__read_config(self.__envInfo, "mian_server_ip")

    def get_main_server_name(self):
        """
        Features: 获取DB部署的机器名称

        :return: main_server_name
        """
        return self.__read_config(self.__envInfo, "main_server_name")

    def get_k8s_bin_path(self):
        """
        Features: 获取k8s模式DB的bin目录

        :return: k8s_bin_path
        """
        return self.__read_config(self.__envInfo, "db_bin_path")

    def get_pod_namespace(self):
        """
        Features: 获取k8s的namespace

        :return: pod_namespace
        """
        return self.__read_config(self.__envInfo, "pod_namespace")

    def get_driver(self):
        """
        Features: 获取ldb的driver的类路径
        :return: driver
        """
        return self.__read_config(self.__envInfo, "driver")

    def get_db_jdbc_url(self):
        """
        Features: 获取ldb的jdbc连接串
        :return: db_jdbc_url
        """
        return self.__read_config(self.__envInfo, "db_jdbc_url")

    def get_meta_db_jdbc_url(self):
        """
        Features: 获取ldb的源数据库jdbc连接串
        :return: meta_db_jdbc_url
        """
        return self.__read_config(self.__envInfo, "meta_db_jdbc_url")

    def get_ldb_driver_file(self):
        """
        Features: 获取ldb的jdbc连接串
        :return: db_jdbc_url
        """
        return glob.glob(self.__driverFilePath + "linkoopdb-jdbc-{0}.jar".format(self.__read_config(self.__envInfo, "driver_version")))

    def get_db_user(self):
        """
        Features: 获取ldb的jdbc登录用户
        :return: db_user
        """
        return self.__read_config(self.__envInfo, "db_user")

    def get_db_pwd(self):
        """
        Features: 获取ldb的jdbc登录密码
        :return: db_pwd
        """
        return self.__read_config(self.__envInfo, "db_pwd")

    def get_db_conf_path(self):
        """
        Features: 获取ldb的conf路径
        :return: db_conf_path
        """
        return self.__read_config(self.__envInfo, "db_conf_path")

    def get_log_level(self):
        """
        Features: 获取打印日志级别信息
        :return: log_level
        """
        return self.__read_config(self.__envInfo, "log_level")

    def get_db_jdbc_url_template(self):
        """
        Features: 获取db_jdbc_url_template
        :return: db_jdbc_url_template
        """
        return self.__read_config(self.__envInfo, "db_jdbc_url_template")