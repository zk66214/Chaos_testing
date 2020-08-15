# -*- coding:utf-8 -*-

import paramiko
from common import Log
from config_utils import parse_config

#由于 paramiko 模块内部依赖pycrypto，所以先下载安装pycrypto
#sudo yum -y install python3-devel
#pip3 install pycrypto
#pip3 install paramiko

class Shell:
    def __init__(self, ip='', username='', password=''):
        self.__username = username
        self.__password = password
        self.__sys_ip = ip
        self.__log = Log.MyLog()
        self.__config = parse_config.ConfigUtil("LOG")
        self.__log_level = self.__config.get_log_level()

    #ssh连接linux
    def get_ssh(self):
        try:
            # 创建ssh客户端
            client = paramiko.SSHClient()
            # 第一次ssh远程时会提示输入yes或者no
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # 密码方式远程连接
            client.connect(self.__sys_ip, 22, username=self.__username, password=self.__password, timeout=20)
            # 互信方式远程连接
            # key_file = paramiko.RSAKey.from_private_key_file("/root/.ssh/id_rsa")
            # ssh.connect(sys_ip, 22, username=username, pkey=key_file, timeout=20)
            return client
        except Exception as e:
            self.__log.error('连接DB=[{0}:{1}/{2}]失败！异常信息：{3}'.format(self.__sys_ip, self.__username, self.__password, str(e)))

    #执行shell命令
    def execShell(self, command):
        client = self.get_ssh()
        try:

            self.__log.info('服务器{0}上执行shell命令：{1}'.format(self.__sys_ip, command))
            #执行命令
            stdin, stdout, stderr = client.exec_command(command)
            #获取命令执行结果,返回的数据是一个list
            result = stdout.readlines()
            if str(self.__log_level).upper() == "DEBUG":
                self.__log.info('运行结果：')
                for line in result:
                    self.__log.info(line)

            return result
        except Exception as e:
            self.__log.error('执行shell命令{0}失败，异常信息：{1}'.format(command, str(e)))
        finally:
            client.close()