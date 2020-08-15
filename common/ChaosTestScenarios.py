from config_utils.parse_config import ConfigUtil
from common import Log
from common.Linkoopdb import *
from common.ChaosBehaviors import *
from common.RemoteHost import *


class ChaosTestScenarios:
    def __init__(self, test_env='NODE66'):
        self.__env = test_env
        self.__chaos = Chaos(self.__env)
        self.__log = Log.MyLog()


    def kill_main_server(self):
        linkoopdb = Linkoopdb(self.__env)
        main_server_ip = linkoopdb.k8s_model.get_main_server_ip()
        if  main_server_ip:
            self.__chaos.PodChaos.pod_kill()
        else:
            self.__log.error('error')



    def kill_one_backup_server(self):
        linkoopdb = Linkoopdb(self.__env)
        backup_server_ips = linkoopdb.k8s_model.get_backup_server_ips()

        if backup_server_ips:
            backup_server_random_ip = backup_server_ips.pop()
            self.__log.info(backup_server_random_ip)
        else:
            self.__log.error('Can not get backup server ip list, please check the test enviroment and ensure the main server is active')

    def kill_all_backup_server(self):
        linkoopdb = Linkoopdb(self.__env)
        backup_server_ips = linkoopdb.k8s_model.get_backup_server_ips()
        if backup_server_ips:
            self.__log.info(backup_server_ips)
        else:
            self.__log.error('Can not get backup server ip list, please check the test enviroment and ensure the main server is active')


if __name__=="__main__":
    remoteMachine = RemoteHost('192.168.1.64', 'root', '123456')
    remoteMachine.kubectl.pod.get_server_pods()

    chaosTestEnv = ChaosTestScenarios('NODE66')
    chaosTestEnv.kill_main_server()
