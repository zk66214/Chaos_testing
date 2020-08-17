from config_utils.parse_config import ConfigUtil
from common import Log
from common.Linkoopdb import *
from common.ChaosBehaviors import *
from common.RemoteHost import *
from common.LDB_ChaosTest import *
# import common.ExceptionHandler
import random


class ChaosTestScenarios:
    def __init__(self, test_env=None):
        self.__env = test_env if test_env else "DEFAULT"
        self.__chaos = Chaos(self.__env)
        self.__log = MyLog()
        self.__linkoopdb = Linkoopdb(self.__env)


    # def kill_main_server(self):
    #     main_server_ip = self.__linkoopdb.k8s_model.get_main_server_ip()
    #     if  main_server_ip:
    #         self.__chaos.PodChaos.pod_kill()
    #     else:
    #         ExceptionHandler.raise_up('error')
    #
    #
    #
    # def kill_one_backup_server(self):
    #     linkoopdb = Linkoopdb(self.__env)
    #     backup_server_ips = linkoopdb.k8s_model.get_backup_server_ips()
    #
    #     if backup_server_ips:
    #         backup_server_random_ip = backup_server_ips.pop()
    #         self.__log.info(backup_server_random_ip)
    #     else:
    #         ExceptionHandler.raise_up('Can not get backup server ip list, please check the test enviroment and ensure the main server is active')
    #
    # def kill_all_backup_server(self):
    #     linkoopdb = Linkoopdb(self.__env)
    #     backup_server_ips = linkoopdb.k8s_model.get_backup_server_ips()
    #     if backup_server_ips:
    #         self.__log.info(backup_server_ips)
    #     else:
    #         ExceptionHandler.raise_up('Can not get backup server ip list, please check the test enviroment and ensure the main server is active')

    """
    创建pallas节点，并验证其状态为running
    :param pallas_name: pallas节点名称
    :param pallas_port: pallas节点端口号
    :param pallas_path: pallas节点本地路径
    :param pallas_host: pallas节点所在机器名称
    :return: True/False
    """
    def enable_ldb_primary_server_failure(self, p_szNameSpace=None):
        ldb_primary_server_pod = self.__linkoopdb.k8s_model.get_db_primary_server_pod()

        try:
            LDB_ChaosTest.disable_pod_failure(ldb_primary_server_pod, p_szNameSpace)
            self.__log.info('Make ldb primary server abnormal successfully')
            return True
        except LDB_ChaosTestException as e:
            ExceptionHandler.raise_up('Failed to make the ldb primary server abnormal, exception info : %s' % e.message)
            raise e.message

    def disable_ldb_primary_server_failure(self, p_szNameSpace=None):
        ldb_primary_server_pod = self.__linkoopdb.k8s_model.get_db_primary_server_pod()

        try:
            LDB_ChaosTest.enable_pod_failure(ldb_primary_server_pod, p_szNameSpace)
            self.__log.info('Make ldb primary server back to stable successfully')
            return True
        except LDB_ChaosTestException as e:
            ExceptionHandler.raise_up('Failed to make ldb primary server back to stable, exception info : %s' % e.message)

    def enable_ldb_backup_server_failure(self, p_szNameSpace=None):
        ldb_backup_server_pods = self.__linkoopdb.k8s_model.get_backup_server_ips()

        ldb_backup_server_pod = ldb_backup_server_pods[random.randint(1, len(ldb_backup_server_pods))]

        try:
            LDB_ChaosTest.enable_pod_failure(ldb_backup_server_pod, p_szNameSpace)
            self.__log.info('Make ldb backup server abnormal successfully')
            return True
        except LDB_ChaosTestException as e:
            ExceptionHandler.raise_up('Failed to make the ldb server abnormal, exception info: %s' % e.message)

    def disable_ldb_backup_server_failure(self, p_szNameSpace=None):
        ldb_backup_server_pods = self.__linkoopdb.k8s_model.get_backup_server_ips()

        ldb_backup_server_pod = ldb_backup_server_pods[random.randint(1, len(ldb_backup_server_pods))]

        try:
            LDB_ChaosTest.enable_pod_failure(ldb_backup_server_pod, p_szNameSpace)
            self.__log.info('Make ldb backup server back to stable successfully')
            return True
        except LDB_ChaosTestException as e:
            ExceptionHandler.raise_up('Failed to make ldb backup server back to stable, exception info : %s' % e.message)



if __name__=="__main__":
    ldb = Linkoopdb()
    ldb.k8s_model.get_db_server_pods()

    chaosTestEnv = ChaosTestScenarios('NODE66')
    chaosTestEnv.disable_ldb_backup_server_failure()
