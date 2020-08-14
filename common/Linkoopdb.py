# -*- coding:utf-8 -*-

from config_utils import parse_config
from common import RemoteMachine
from common import Log
from common import Sql
from common import Linux
import time
from jpype import *
import jaydebeapi

class Init:
    def __init__(self, test_env='P1-K8S-ENV'):
        self.__env = test_env

    @property
    def k8s_model(self):
        return _K8SInit(self.__env)

    @property
    def local_model(self):
        return _LocalInit(self.__env)

# K8S安装模式下的操作
class _K8SInit:
    def __init__(self, test_env='P1-K8S-ENV'):
        self.__test_env = test_env
        self.__config = parse_config.ConfigUtil(self.__test_env)
        self.__k8s_bin = self.__config.get_k8s_bin_path()
        self.__linux = RemoteMachine.Init(self.__config.get_main_server_ip(), self.__config.get_authority_user(), self.__config.get_authority_pwd())
        self.__shell = Linux.Init(self.__config.get_main_server_ip(), self.__config.get_authority_user(), self.__config.get_authority_pwd())

        self.__log = Log.MyLog()


    def get_main_server_ip(self):
        # driver = self.__config.get_ldb_driver_file()
        # try:
        #     startJVM(getDefaultJVMPath(), "-ea", "-Djava.class.path=%s" % (driver))  # 启动jvm
        # except:
        #     pass
        # java.lang.System.out.println('Success')
        # ClusterStateUtils = JClass("com.datapps.linkoopdb.jdbc.util.web.ClusterStateUtils")
        #
        # host_ips = str(self.__config.get_host_ips()).split(',')
        # main_server_ip = ClusterStateUtils().queryMainServerUrl(host_ips)  # 创建类的实例，可以调用类里边的方法
        # shutdownJVM()
        # try:
        #     driver = self.__config.get_ldb_driver_file()
        #     startJVM(getDefaultJVMPath(), "-ea", "-Djava.class.path=%s" % (driver))  # 启动jvm
        #     java.lang.System.out.println('Success')
        #     ClusterStateUtils = JClass("com.datapps.linkoopdb.jdbc.util.web.ClusterStateUtils")
        #     ip_list = []
        #     ip_list.append()
        #     host_ips = str(self.__config.get_host_ips()).split(',')
        #     main_server_ip = ClusterStateUtils().queryMainServerUrl(host_ips) # 创建类的实例，可以调用类里边的方法
        # except Exception:
        #     self.__log.error('获取主server ip失败')
        # else:
        #     return main_server_ip
        # finally:
        #     shutdownJVM()  # 最后关闭jvm

        driver = self.__config.get_driver()

        user = self.__config.get_db_user()
        password = self.__config.get_db_pwd()
        jar = self.__config.get_ldb_driver_file()
        conn = None
        for ip in str(self.__config.get_host_ips()).split(','):
            url = str.replace(self.__config.get_db_jdbc_url_template(), '{ip}', ip)
            # conn = jaydebeapi.connect(driver, url, [user, password], jar, )
            try:
                conn = jaydebeapi.connect(driver, url, [user, password], jar, )
            except Exception:
                pass
            else:
                return ip
            finally:
                if conn:
                    conn.close()
        pass


    """
    创建pallas节点，并验证其状态为running
    :param pallas_name: pallas节点名称
    :param pallas_port: pallas节点端口号
    :param pallas_path: pallas节点本地路径
    :param pallas_host: pallas节点所在机器名称
    :return: True/False
    """
    def create_db_pallas_node(self, pallas_name='', pallas_port='', pallas_path='', pallas_host=''):
        try:
            if not pallas_name:
                self.__log.error('没有提供pallas_name!')

            if not pallas_port:
                self.__log.error('没有提供pallas_port!')

            if not pallas_path:
                self.__log.error('没有提供pallas_path!')

            if not pallas_host:
                self.__log.error('没有提供pallas_host!')

            sql = Sql.Init(self.__test_env)

            #判断节点是否存在，若存在，直接报错
            #result = sql.run(
            #    "select * from information_schema.storage_nodes WHERE HOST='{0}' AND PORT='{1}'".format(pallas_host, pallas_port))
            #if result:
            #    raise Exception('pallas节点已存在，创建pallas节点失败！')

            #判断pallas节点的路径是否存在，若已存在，删除改目录
            host = RemoteMachine.Init(pallas_host, self.__config.get_authority_user(), self.__config.get_authority_pwd())
            if host.dir.exists(pallas_path):
                host.dir.delete(pallas_path)

            # 创建节点
            self.__log.info('开始创建db pallas节点：{0}'.format(pallas_name))
            result = sql.run(
                "CREATE pallas \"{0}\" port {1} storage '{2}' node affinity '{3}'".format(pallas_name, pallas_port, pallas_path, pallas_host))

            #如果执行sql未出错，验证pallas节点状态
            if result == 'No Response':
                # 验证pallas节点是否成功
                base_second = 10
                n = 1
                self.__log.info('【验证新增pallas节点状态】')
                while n < 5:
                    self.__log.info('第{0}次验证db pallas状态'.format(n))

                    db_pod = self.__linux.kubectl.pod.get_pod_by_name(self.__config.get_pod_namespace(), pallas_name)
                    if db_pod:
                        for (k, v) in db_pod.items():
                            if k == pallas_name and v == 'Running':
                                self.__log.info('成功创建db pallas节点：{0}'.format(pallas_name))
                                return True
                    if n != 4:
                        self.__log.info('sleep {0}s...'.format(n * base_second))
                        time.sleep(n * base_second)
                    n = n + 1

                self.__log.info('第{0}次验证db pallas状态'.format(n))

                db_pod = self.__linux.kubectl.pod.get_pod_by_name(self.__config.get_pod_namespace(), pallas_name)
                if db_pod:
                    for (k, v) in db_pod.items():
                        if k == pallas_name and v == 'Running':
                            self.__log.info('成功创建db pallas节点：{0}'.format(pallas_name))
                            return True
                self.__log.error('创建db pallas节点：{0}失败，请查看对应的pod状态！'.format(pallas_name))
                return False

            self.__log.error('创建db pallas节点：{0}失败！异常信息：{1}'.format(pallas_name, result))
            return False

        except Exception as e:
            self.__log.error('创建db pallas节点：{0}出现错误！，异常信息：{1}'.format(pallas_name, e))
            return False
        finally:
            sql.disconnect()

    """
    下线pallas节点
    :param pallas_name: pallas节点名称
    :param type: offline:临时下线；shutdown：永久下线
    :return: True/False
    """

    def offline_db_pallas_node(self, pallas_name=''):
        try:
            if not pallas_name:
                self.__log.error('没有提供pallas_name!')

            sql = Sql.Init(self.__test_env)
            self.__log.info('开始下线db pallas节点：{0}'.format(pallas_name))
            result = sql.run("ALTER PALLAS {0} SET STATE 2".format(pallas_name))

            # 若带删除pallas节点不存在，验证pallas节点是否成功
            if 'May not exist' not in result:
                base_second = 10
                n = 1
                self.__log.info('【验证新增pallas节点状态】')
                while n < 5:
                    self.__log.info('第{0}次验证db pallas状态'.format(n))

                    db_pod = self.__linux.kubectl.pod.get_pod_by_name(self.__config.get_pod_namespace(), pallas_name)
                    if not db_pod:
                        self.__log.info('成功下线db pallas节点：{0}'.format(pallas_name))
                        return True
                    if n != 4:
                        self.__log.info('sleep {0}s...'.format(n * base_second))
                        time.sleep(n * base_second)
                    n = n + 1

                self.__log.error('下线db pallas节点：{0}失败！'.format(pallas_name))
                return False

        except Exception as e:
            self.__log.error('临时下线db pallas节点：{0}出现错误！，异常信息：{1}'.format(pallas_name, e))
            return False
        finally:
            sql.disconnect()


    """
    永久下线pallas节点，并删除对应的本地数据
    :param pallas_name: pallas节点名称
    :param host: pallas节点所在机器
    :param port: pallas节点占用的端口
    :return: True/False
    """

    def shutdown_db_pallas_node(self, pallas_name='', host='', port=''):
        try:
            if not pallas_name:
                self.__log.error('没有提供pallas_name!')
                return False

            if not host:
                self.__log.error('没有提供host!')
                return False

            if not port:
                self.__log.error('没有提供port!')
                return False

            sql = Sql.Init(self.__test_env)

            self.__log.info('开始下线db pallas节点：{0}'.format(pallas_name))
            #若节点的状态为2，恢复为1后才可以永久删除，否则会报错
            pallas_node = sql.run("select STATE, STORAGE_PATH from information_schema.storage_nodes WHERE HOST='{0}' AND PORT='{1}'".format(host, port))
            #若pallas节点不存在，直接返回
            if not pallas_node:
                self.__log.info('db pallas节点不存在：{0}'.format(pallas_name))
                return True

            #若pallas节点状态为2，重新启动节点
            if pallas_node[0][0] == 2:
                self.create_db_pallas_node(pallas_name=pallas_name, pallas_port=port, pallas_path=pallas_node[0][1], pallas_host=host)

            result = sql.run("ALTER PALLAS {0} SET STATE 0".format(pallas_name))

            # 若带删除pallas节点不存在，验证pallas节点是否成功
            if 'May not exist' not in result:
                base_second = 10
                n = 1
                self.__log.info('【验证新增pallas节点状态】')
                while n < 5:
                    self.__log.info('第{0}次验证db pallas状态'.format(n))

                    db_pod = self.__linux.kubectl.pod.get_pod_by_name(self.__config.get_pod_namespace(), pallas_name)
                    if not db_pod:
                        self.__log.info('成功下线db pallas节点：{0}'.format(pallas_name))
                        return True
                    if n != 4:
                        self.__log.info('sleep {0}s...'.format(n * base_second))
                        time.sleep(n * base_second)
                    n = n + 1

                self.__log.error('下线db pallas节点：{0}失败！'.format(pallas_name))
                return False

            linux = RemoteMachine.Init(host, self.__config.get_authority_user(), self.__config.get_authority_pwd())
            linux.dir.delete(pallas_node[0][1])
            self.__log.info('成功删除远程机器{0}上的pallas节点{1}的本地路径{2}'.format(host, pallas_name, pallas_node[0][1]))
        except Exception as e:
            self.__log.error('临时下线db pallas节点：{0}出现错误！，异常信息：{1}'.format(pallas_name, e))
            return False
        finally:
            sql.disconnect()




    """
    restart db
    :param pallas_name_list: pallas节点信息，以list格式存储pallas节点对应的pod名称
    :return: True/False
    """

    def restart_db(self, pallas_name_list=[]):
        self.stop_db(pallas_name_list)
        self.start_db(pallas_name_list)

    """
    start db
    :param pallas_name_list: pallas节点信息，以list格式存储pallas节点对应的pod名称
    :return: True/False
    """
    def start_db(self, pallas_name_list=[]):
        start_command = '{0}/ldb-k8s.sh start'.format(self.__config.get_k8s_bin_path())
        self.__log.info('开始启动DB，执行命令：{0}'.format(start_command))
        self.__shell.runShell(start_command)

        # 1、等待nfs server启动成功
        if not self.verify_nfs_server_started():
            return False

        # 2、等待meta启动成功
        if not self.verify_meta_server_started():
            return False

        # 3、等待db启动成功
        if not self.verify_server_started():
            return False

        # 4、等待meta pallas启动成功
        if not self.verify_meta_pallas_started():
            return False

        # 5、等待meta worker启动成功
        if not self.verify_meta_worker_started():
            return False

        # 6、等待db worker启动成功
        if not self.verify_db_worker_started():
            return False

        # 7、等待pallas节点启动成功
        if not self.verify_db_pallas_started(pallas_name_list):
            return False

        print('启动linkoopdb成功！')
        return True

    """
    stop db
    :param pallas_name_list: pallas节点信息，以list格式存储pallas节点对应的pod名称
    :return: True/False
    """
    def stop_db(self, pallas_name_list=[]):
        self.__shell.runShell('{0}/ldb-k8s.sh stop'.format(self.__config.get_k8s_bin_path()))

        # 1、等待pallas节点停止成功
        if not self.verify_db_pallas_stopped(pallas_name_list):
            return False

        # 2、等待db停止成功
        if not self.verify_server_stopped():
            return False

        # 3、等待meta停止成功
        if not self.verify_meta_server_stopped():
            return False

        # 4、等待meta pallas停止成功
        if not self.verify_meta_pallas_stopped():
            return False

        # 5、等待nfs server停止成功
        if not self.verify_nfs_server_stopped():
            return False

        # 6、等待db worker停止成功
        if not self.verify_db_worker_stopped():
            return False

        # 7、等待meta worker停止成功
        if not self.verify_meta_worker_stopped():
            return False

        print('停止linkoopdb成功！')
        return True

    """
    判断nfs server节点状态，若节点个数不为1或任一pod状态不为running，则返回False
    :param namespace: namespace
    :return: True/False
    """
    def verify_nfs_server_started(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        n = 0
        base_second = 30
        self.__log.info('【验证nfs server节点状态】')
        while n < 3:
            n = n + 1
            self.__log.info('第{0}次验证nfs server节点状态'.format(n))

            nfs_pod = self.__linux.kubectl.pod.get_nfs_server_pods(namespace)
            # 若pod数量不等于1,则跳过本次验证
            if len(nfs_pod) == 1:
                # 判断pod状态是否为running
                for (k, v) in nfs_pod.items():
                    if v == 'Running':
                        self.__log.info('nfs server启动成功')
                        return True
                    else:
                        self.__log.warning('nfs server状态不是Running！')
                        continue
            else:
                self.__log.warning('nfs server数量不为1！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('nfs server启动失败')
        return False

    """
    判断meta pallas节点状态，若节点个数不为4或任一pod状态不为running，则返回False
    :param namespace: namespace
    :return: True/False
    """
    def verify_meta_pallas_started(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        n = 0
        base_second = 30
        self.__log.info('【验证meta pallas节点状态】')
        while n < 3:
            n = n + 1
            success = 0
            self.__log.info('第{0}次验证meta server节点状态'.format(n))

            db_pods = self.__linux.kubectl.pod.get_meta_pallas_pods(namespace)
            # 若pod个数不为4,则跳过本次验证
            if len(db_pods) == 4:
                # 判断pod状态是否为running
                for (k, v) in db_pods.items():
                    if v == 'Running':
                        success = success + 1
                    else:
                        continue

            if success == 4:
                self.__log.info('meta pallas启动成功')
                return True
            else:
                self.__log.warning('meta pallas数量不为4！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('meta pallas启动失败')
        return False

    """
    判断meta server节点状态，若节点个数不为3或任一pod状态不为running，则返回False
    :param namespace: namespace
    :return: True/False
    """
    def verify_meta_server_started(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        n = 0
        base_second = 30
        self.__log.info('【验证meta server节点状态】')
        while n < 3:
            n = n + 1
            success = 0
            self.__log.info('第{0}次验证meta server节点状态'.format(n))

            db_pods = self.__linux.kubectl.pod.get_meta_server_pods(namespace)
            # 若pod个数不为3,则跳过本次验证
            if len(db_pods) == 3:
                # 判断pod状态是否为running
                for (k, v) in db_pods.items():
                    if v == 'Running':
                        success = success + 1
                    else:
                        continue

            if success == 3:
                self.__log.info('meta server启动成功')
                return True
            else:
                self.__log.warning('meta server数量不为3！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('meta server启动失败')
        return False

    """
    判断server节点状态，若节点个数不为3或任一pod状态不为running，则返回False
    :param namespace: namespace
    :return: True/False
    """
    def verify_server_started(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        n = 0
        base_second = 30
        self.__log.info('【验证db server节点状态】')
        while n < 3:
            n = n + 1
            success = 0
            self.__log.info('第{0}次验证db server节点状态'.format(n))

            db_pods = self.__linux.kubectl.pod.get_server_pods(namespace)
            # 若pod个数不为3,则跳过本次验证
            if len(db_pods) == 3:
                # 判断pod状态是否为running
                for (k, v) in db_pods.items():
                    if v == 'Running':
                        success = success + 1
                    else:
                        continue

            if success == 3:
                self.__log.info('db server启动成功')
                return True
            else:
                self.__log.warning('db server数量不为3！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('db server启动失败')
        return False

    """
    判断pallas节点的状态，若节点个数不够或任一pod状态不为running，则返回False
    :param namespace: namespace
    :param pod_name_list: pod_name_list
    :return: True/False
    """
    def verify_db_pallas_started(self, pod_name_list=[], namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        if not pod_name_list:
            self.__log.info('没有pallas节点，跳过验证')
            return True
        self.__log.info('【验证db pallas节点状态】')
        pod_count = len(pod_name_list)
        self.__log.info('等待{0}个pod启动，sleep {1}s...'.format(pod_count, pod_count * 10))
        time.sleep(pod_count * 5)

        n = 0
        base_second = 30

        while n < 4:
            n = n + 1
            success = 0
            self.__log.info('第{0}次验证pallas节点状态'.format(n))

            pod_list = self.__linux.kubectl.pod.get_db_pallas_pods(namespace)
            # 若pod数量不匹配
            if pod_count != len(pod_list):
                self.__log.warning('当前pallas节点为{0}不等于pod个数{1}'.format(len(pod_list), pod_count))
                print('sleep {0}s...'.format(n * base_second))
                time.sleep(n * base_second)
                continue

            for pod_name in pod_name_list:
                if pod_list[pod_name] == 'Running':
                    success = success + 1
                else:
                    break

            #若pod状态不全是running
            if success != pod_count:
                self.__log.warning('部分pallas节点未启动成功')
                self.__log.info('sleep {0}s...'.format(n * base_second))
                continue

            self.__log.info('{0}个pallas节点启动成功'.format(pod_count))
            return True

        self.__log.error('pallas节点启动失败!')
        return False

    """
    判断meta worker状态，若状态不为running，则返回False
    :param user: application的user信息
    :return: True/False
    """
    def verify_meta_worker_started(self, user=''):
        if not user:
            self.__log.info('若未指定user，使用默认值：{0}'.format(self.__config.get_authority_user()))
            user = self.__config.get_authority_user()

        base_second = 30
        n = 0
        self.__log.info('【验证meta worker状态】')
        while n < 6:
            n = n + 1
            self.__log.info('第{0}次验证meta worker状态'.format(n))

            app_info = self.__linux.yarn.get_appid_by_user_and_appname(user, 'linkoopmeta-worker-DEFAULT')
            if app_info:
                if self.__linux.yarn.get_app_state(app_info) == 'RUNNING':
                    self.__log.info('meta worker启动成功')
                    return True

            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        if n == 5:
            self.__log.error('meta worker启动失败！')
            return False

    """
    判断db worker状态，若状态不为running，则返回False
    :param user: application的user信息
    :return: True/False
    """
    def verify_db_worker_started(self, user=''):
        if not user:
            self.__log.info('若未指定user，使用默认值：{0}'.format(self.__config.get_authority_user()))
            user = self.__config.get_authority_user()

        base_second = 30
        n = 0
        self.__log.info('【验证db worker状态】')
        while n < 6:
            n = n + 1
            self.__log.info('第{0}次验证db worker状态'.format(n))

            app_info = self.__linux.yarn.get_appid_by_user_and_appname(user, 'linkoopdb-worker-DEFAULT')
            if app_info:
                if self.__linux.yarn.get_app_state(app_info) == 'RUNNING':
                    self.__log.info('db worker启动成功')
                    return True

            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        if n == 5:
            self.__log.error('db worker启动失败！')
            return False

    """
    判断meta pallas节点状态，若节点未全部删除，则返回False
    :param namespace: namespace
    :return: True/False
    """
    def verify_meta_pallas_stopped(self, namespace=''):
        if not namespace:
            self.__log.info('若未指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        base_second = 30
        n = 0
        self.__log.info('【验证meta pallas状态】')
        while n < 3:
            n = n + 1
            self.__log.info('第{0}次验证meta pallas状态'.format(n))

            db_pods = self.__linux.kubectl.pod.get_meta_pallas_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('meta pallas关闭成功')
                return True

            self.__log.warning('meta pallas的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('meta pallas关闭失败！')
        return False

    """
    判断meta server节点状态，若节点未全部删除，则返回False
    :param namespace: namespace
    :return: True/False
    """
    def verify_meta_server_stopped(self, namespace=''):
        if not namespace:
            self.__log.info('若未指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        base_second = 30
        n = 0
        self.__log.info('【验证meta server状态】')
        while n < 3:
            n = n + 1
            self.__log.info('第{0}次验证meta server状态'.format(n))

            db_pods = self.__linux.kubectl.pod.get_meta_server_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('meta server关闭成功')
                return True

            self.__log.warning('meta server的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('meta server关闭失败！')
        return False

    """
    判断server节点状态，若节点未全部删除，则返回False
    :param namespace: namespace
    :return: True/False
    """
    def verify_server_stopped(self, namespace=''):
        if not namespace:
            self.__log.info('若未指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        base_second = 30
        n = 0
        self.__log.info('【验证db server状态】')
        while n < 3:
            n = n + 1
            self.__log.info('第{0}次验证db server状态'.format(n))

            db_pods = self.__linux.kubectl.pod.get_server_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('db server关闭成功')
                return True

            self.__log.warning('db server的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('db server关闭失败！')
        return False

    """
    判断db pallas节点的状态，若节点未全部删除，则返回False
    :param namespace: namespace
    :return: True/False
    """
    def verify_db_pallas_stopped(self, namespace=''):
        if not namespace:
            self.__log.info('若未指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        base_second = 30
        n = 0
        self.__log.info('【验证db pallas状态】')
        while n < 3:
            n = n + 1
            self.__log.info('第{0}次验证db pallas状态'.format(n))

            db_pods = self.__linux.kubectl.pod.get_db_pallas_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('db pallas关闭成功')
                return True

            self.__log.warning('db pallas的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('db pallas关闭失败！')
        return False
    """
    判断nfs server节点状态，若节点个数不为4或任一pod状态不为running，则返回False
    :param namespace: namespace
    :return: True/False
    """
    def verify_nfs_server_stopped(self, namespace=''):
        if not namespace:
            self.__log.info('若未指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        base_second = 30
        n = 0
        self.__log.info('【验证nfs server状态】')
        while n < 3:
            n = n + 1
            self.__log.info('第{0}次验证nfs server状态'.format(n))

            db_pods = self.__linux.kubectl.pod.get_nfs_server_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('nfs server关闭成功')
                return True

            self.__log.warning('nfs server的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('nfs server关闭失败！')
        return False

    """
    判断meta worker状态，若状态不为running，则返回False
    :param user: application的user信息
    :return: True/False
    """
    def verify_meta_worker_stopped(self, user=''):
        if not user:
            self.__log.info('若未指定user，使用默认值：{0}'.format(self.__config.get_authority_user()))
            user = self.__config.get_authority_user()

        base_second = 30
        n = 0
        self.__log.info('【验证meta worker状态】')
        while n < 5:
            n = n + 1
            self.__log.info('第{0}次验证meta worker状态'.format(n))

            app_info = self.__linux.yarn.get_appid_by_user_and_appname(user, 'linkoopmeta-worker-DEFAULT')
            if not app_info:
                self.__log.info('meta worker关闭成功')
                return True

            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.warning('meta worker关闭失败！')
        return False

    """
    判断db worker状态，若状态不为running，则返回False
    :param user: application的user信息
    :return: True/False
    """

    def verify_db_worker_stopped(self, user=''):
        if not user:
            self.__log.info('若未指定user，使用默认值：{0}'.format(self.__config.get_authority_user()))
            user = self.__config.get_authority_user()

        base_second = 30
        n = 0
        self.__log.info('【验证db worker状态】')
        while n < 5:
            n = n + 1
            self.__log.info('第{0}次验证db worker状态'.format(n))

            app_info = self.__linux.yarn.get_appid_by_user_and_appname(user, 'linkoopdb-worker-DEFAULT')
            if not app_info:
                self.__log.info('db worker关闭成功')
                return True

            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        self.__log.error('db worker关闭失败！')
        return False

    """
    永久删除db中所有pallas节点
    :return: None
    """
    def clean_db_pallas(self):
        try:
            sql = Sql.Init()
            # 查询现有的pallas节点
            rows = sql.run(
                "select HOST, concat(HOST,PORT) AS PALLAS_NAME, STORAGE_PATH, PORT, STATE from information_schema.storage_nodes")

            # 若rows为None，表示返回数据为空
            if rows == 'No Response':
                self.__log.info('没有Pallas节点，无须删除节点')
            else:
                for row in rows:
                    self.shutdown_db_pallas_node(pallas_name=row[1], host=row[0], port=str(row[3]))

        except Exception as e:
            self.__log.error('删除pallas节点并清空本地路径失败！异常信息：{0}'.format(e))
        finally:
            sql.disconnect()

class _LocalInit:
    def start_db(self):
        pass

    def stop_db(self):
        pass


if __name__=="__main__":
    sql = Sql.Init('NODE66')

    linkoopdb = Init('NODE66')
    ip = linkoopdb.k8s_model.get_main_server_ip()

    pallas_pod_list = ['node649011']
    #
    # #linkoopdb.k8s_model.start_db(pallas_name_list=pallas_pod_list)
    #
    # #linkoopdb.k8s_model.stop_db(pallas_name_list=pallas_pod_list)
    # pallas_name_list = ['node6440001', 'node6440002', 'node6440003', 'node6440004']
    # linkoopdb.k8s_model.verify_db_pallas_started(pallas_name_list)