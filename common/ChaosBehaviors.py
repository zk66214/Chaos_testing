from config_utils.parse_config import ConfigUtil
from common import Log
from common.Shell import *
from common.LDB_ChaosTest import *


class ChaosBehaviors:
    def __init__(self, test_env=None):
        self.__env = test_env

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
    def __init__(self, test_env='NODE66'):
        self.__env = test_env
        self.__config = ConfigUtil(self.__env)
        self.__namespace = self.__config.get_pod_namespace()
        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                                  self.__config.get_authority_pwd())
        self.__log = Log.MyLog()

    def pod_kill(self, pod_name):
        if not pod_name:
            self.__log.error('error')
            return True

        self.__linux.execShell('')

    def enable_pod_failure(self, pod_name=None):
        if not pod_name:
            self.__log.error('error')
            return True

        LDB_ChaosTest.enable_pod_failure(pod_name, self.__namespace)

    def disable_pod_failure(self, pod_name=None):
        if not pod_name:
            self.__log.error('error')
            return True

        LDB_ChaosTest.disable_pod_failure(pod_name, self.__namespace)


class _NetworkChaos:
    def __init__(self, test_env='NODE66'):
        self.__env = test_env
        self.__config = ConfigUtil(self.__env)

        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                                  self.__config.get_authority_pwd())
        self.__log = Log.MyLog()

    def network_split(self, pod_group1, pod_group2):
        if not pod_group1 or not pod_group2:
            self.__log.error('error')
            return True

        self.__linux.execShell('')

    def network_loss(self, pod_group1, pod_group2, loss_rate='30'):
        if not pod_group1 or not pod_group2:
            self.__log.error('error')
            return True

        self.__linux.execShell('')

    def network_delay(self, pod_group1, pod_group2, latency='30'):
        if not pod_group1 or not pod_group2:
            self.__log.error('error')
            return True

        self.__linux.execShell('')

    def network_duplicate(self, pod_group1, pod_group2, duplicate_rate='30'):
        if not pod_group1 or not pod_group2:
            self.__log.error('error')
            return True

        self.__linux.execShell('')

    def network_corrupt(self, pod_group1, pod_group2, corrupt_rate='30'):
        if not pod_group1 or not pod_group2:
            self.__log.error('error')
            return True

        self.__linux.execShell('')

    def network_bandwidth_action(self, pod_group1, pod_group2, limit_buffer='30', limit_rate=''):
        if not pod_group1 or not pod_group2:
            self.__log.error('error')
            return True

        self.__linux.execShell('')

class _IOChaos:
    def __init__(self, test_env='NODE66'):
        self.__env = test_env
        self.__config = ConfigUtil(self.__env)

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
    def __init__(self, test_env='NODE66'):
        self.__env = test_env
        self.__config = ConfigUtil(self.__env)

        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                                  self.__config.get_authority_pwd())
        self.__log = Log.MyLog()

    def time_delay(self, pod_name, delay=''):
        if not pod_name:
            self.__log.error('error')
            return True

        self.__linux.execShell('')


class _CPUChaos:
    def __init__(self, test_env='NODE66'):
        self.__env = test_env
        self.__config = ConfigUtil(self.__env)

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
    def __init__(self, test_env='NODE66'):
        self.__env = test_env
        self.__config = ConfigUtil(self.__env)

        self.__linux = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(),
                                  self.__config.get_authority_pwd())
        self.__log = Log.MyLog()
    pass


if __name__=="__main__":
    Chaos = ChaosBehaviors('NODE66')
    Chaos.PodChaos.enable_pod_failure("linkoopdb-database-0")
