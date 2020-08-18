from config_utils.parse_config import ConfigUtil
from common import Log
from common.Shell import *
from common.LDB_ChaosTest import *
from common.ExceptionHandler import *


class ChaosBehaviors:
    def __init__(self, test_env=None):
        self.__env = test_env if test_env else "DEFAULT"

    @property
    def PodChaos(self):
        return _PodChaos(self.__env)

    @property
    def NetworkChaos(self):
        return _NetworkChaos(self.__env)

    @property
    def IOChaos(self):
        return _IOChaos(self.__env)

    @property
    def TimeChaos(self):
        return _TimeChaos(self.__env)

    @property
    def KernelChaos(self):
        return _KernelChaos(self.__env)


class _PodChaos:
    def __init__(self, test_env=None):
        self.__env = test_env if test_env else "DEFAULT"

        self.__config = ConfigUtil(self.__env)
        self.__namespace = self.__config.get_pod_namespace()
        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                                  self.__config.get_authority_pwd())
        self.__log = Log.MyLog()

    def pod_kill(self, pod_name):
        if not pod_name:
            ExceptionHandler.raise_up('Failed to kill pod, please provide the name of target pod')

        LDB_ChaosTest.pod_kill(pod_name, self.__namespace)

    def enable_pod_failure(self, pod_name=None):
        if not pod_name:
            ExceptionHandler.raise_up('Failed to enable pod failure, please provide the name of target pod')

        LDB_ChaosTest.enable_pod_failure(pod_name, self.__namespace)

    def disable_pod_failure(self, pod_name=None):
        if not pod_name:
            ExceptionHandler.raise_up('Failed to disable pod failure, please provide the name of target pod')

        LDB_ChaosTest.disable_pod_failure(pod_name, self.__namespace)


class _NetworkChaos:
    def __init__(self, test_env=None):
        self.__env = test_env if test_env else "DEFAULT"
        self.__config = ConfigUtil(self.__env)
        self.__namespace = self.__config.get_pod_namespace()
        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                                  self.__config.get_authority_pwd())
        self.__log = Log.MyLog()

    def enable_network_split(self, node_group=None, target_node_group=None):
        if not node_group or not target_node_group:
            ExceptionHandler.raise_up('Failed to enable network split, please provide the node group or target node group')

        LDB_ChaosTest.enable_network_split(node_group, target_node_group, self.__namespace)

    def disable_network_split(self, node_group=None, target_node_group=None):
        if not node_group or not target_node_group:
            ExceptionHandler.raise_up(
                'Failed to disable network split, please provide the node group or target node group')

        LDB_ChaosTest.disable_network_split(node_group, target_node_group, self.__namespace)

    def enable_network_delay(self, node_group=None):
        if not node_group:
            ExceptionHandler.raise_up(
                'Failed to enable network delay, please provide the node group')

        LDB_ChaosTest.enable_network_delay(node_group, self.__namespace)

    def disable_network_delay(self, node_group=None):
        if not node_group:
            ExceptionHandler.raise_up(
                'Failed to disable network delay, please provide the node group')

        LDB_ChaosTest.disable_network_delay(node_group, self.__namespace)


class _IOChaos:
    def __init__(self, test_env=None):
        self.__env = test_env if test_env else "DEFAULT"

        self.__config = ConfigUtil(self.__env)
        self.__namespace = self.__config.get_pod_namespace()
        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                             self.__config.get_authority_pwd())
        self.__log = Log.MyLog()

    def io_delay(self, pod_name, delay=''):
        if not pod_name:
            self.__log.error('error')
            return True

        self.__linux.execShell('')

    def io_error(self, pod_name, rate=''):
        if not pod_name:
            self.__log.error('error')
            return True

        self.__linux.execShell('')

class _TimeChaos:
    def __init__(self, test_env=None):
        self.__env = test_env if test_env else "DEFAULT"

        self.__config = ConfigUtil(self.__env)
        self.__namespace = self.__config.get_pod_namespace()
        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                             self.__config.get_authority_pwd())
        self.__log = Log.MyLog()

    def time_delay(self, pod_name, delay=''):
        if not pod_name:
            self.__log.error('error')
            return True

        self.__linux.execShell('')


class _CPUChaos:
    def __init__(self, test_env=None):
        self.__env = test_env if test_env else "DEFAULT"

        self.__config = ConfigUtil(self.__env)
        self.__namespace = self.__config.get_pod_namespace()
        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                             self.__config.get_authority_pwd())
        self.__log = Log.MyLog()

    def cpu_stress(self, pod_name, cpu_count=''):
        if not pod_name:
            self.__log.error('error')
            return True

        self.__linux.execShell('')
    pass

class _KernelChaos:
    def __init__(self, test_env=None):
        self.__env = test_env if test_env else "DEFAULT"

        self.__config = ConfigUtil(self.__env)
        self.__namespace = self.__config.get_pod_namespace()
        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                             self.__config.get_authority_pwd())
        self.__log = Log.MyLog()
    pass


if __name__=="__main__":
    Chaos = ChaosBehaviors()
    Chaos.PodChaos.enable_pod_failure("linkoopdb-database-0")
