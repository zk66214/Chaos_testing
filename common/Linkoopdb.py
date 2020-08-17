# -*- coding:utf-8 -*-

from config_utils import parse_config
from common import RemoteHost
from common.Log import *
from common.Sql import *
from common.Shell import *
from common.RemoteHost import *
from common.K8SHandler import *
from common.EnumPodLabel import *
from common.RemoteHost import *
from common.ExceptionHandler import *
import time
import jaydebeapi

from common.K8SHandler import *

# class LinkoopdbException(Exception):
#     def __init__(self, message):
#         Exception.__init__(self)
#         self.message = message

class Linkoopdb:
    def __init__(self, test_env=''):
        self.__env = test_env

    @property
    def k8s_model(self):
        return _K8SInit(self.__env)

    # @property
    # def local_model(self):
    #     return _LocalInit(self.__env)

    @property
    def host(self):
        return _HostInit(self.__env)

    @property
    def yarn(self):
        return _YarnInit(self.__env)

# K8S安装模式下的操作
class _K8SInit:
    def __init__(self, test_env=None):
        #若用户没有定义test_env，使用配置文件中的DEFAULT
        self.__test_env = test_env if test_env else parse_config.ConfigUtil("DEFAULT").get_config_name()
        self.__config = parse_config.ConfigUtil(self.__test_env)

        #初始化default_config
        self.__default_config = parse_config.ConfigUtil("DEFAULT")

        # 初始化K8SHandler
        self.__k8s_handler = K8SHandler(self.__default_config.get_k8s_api_server(), self.__default_config.get_k8s_api_token())

        self.__k8s_bin = self.__config.get_k8s_bin_path()
        self.__linux = RemoteHost(self.__config.get_main_server_ip(), self.__config.get_authority_user(), self.__config.get_authority_pwd())
        self.__shell = Shell(self.__config.get_main_server_ip(), self.__config.get_authority_user(), self.__config.get_authority_pwd())
        self.__yarn = Linkoopdb.yarn(self.__test_env)
        self.__log = Log.MyLog()

    """
    查询db primary server pod的相关信息
    :return: 返回pod对象的list或None
    """
    def get_db_primary_server_pod(self, namespace=None):
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
        #     ExceptionHandler.raise_up('获取主server ip失败')
        # else:
        #     return main_server_ip
        # finally:
        #     shutdownJVM()  # 最后关闭jvm

        primary_ip = ''
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
                primary_ip = ip
            finally:
                if conn:
                    conn.close()
        if not primary_ip:
            ExceptionHandler.raise_up('can not obtain the ip of primary ldb server pod')

        pod_namespace = namespace if namespace else self.__config.get_pod_namespace()


        pod_list = K8SHandler.List_Pods(pod_namespace)
        for pod in pod_list:
            if pod.pod_ip == primary_ip:
                return pod

        ExceptionHandler.raise_up('can not obtain the info of primary ldb server pod')

    """
    查询db backup server pod的相关信息
    :return: 返回pod对象的list或None
    """
    def get_db_backup_server_pod(self, namespace=None):
        pod_namespace = namespace if namespace else self.__config.get_pod_namespace()

        ldb_server_pods = self.get_db_server_pods(pod_namespace)

        ldb_primary_server_pod = self.get_db_primary_server_pod(pod_namespace)

        if ldb_primary_server_pod:
            ldb_server_pods.remove(ldb_primary_server_pod)
            return ldb_primary_server_pod
        else:
            ExceptionHandler.raise_up('can not obtain the info of backup ldb server pod')

    """
    查询db server pod的相关信息
    :return: 返回pod对象的list或None
    """
    def get_db_server_pods(self, namespace=None):
        namespace = namespace if namespace else self.__config.get_pod_namespace()
        self.__log.info('namespace：%s' % namespace)

        #获取label信息
        label_dict = PodLabel.get_labels_via_name(PodLabel.SERVER)

        return self.__k8s_handler.List_Pods_By_Labels(p_szNameSpace=namespace, labels=label_dict)

    """
    查询meta server pod的相关信息
    :return: 返回pod对象的list或None
    """
    def get_meta_server_pods(self, namespace=None):
        namespace = namespace if namespace else self.__config.get_pod_namespace()
        self.__log.info('namespace：%s' % namespace)

        # 获取label信息
        label_dict = PodLabel.get_labels_via_name(PodLabel.META_SERVER)

        return self.__k8s_handler.List_Pods_By_Labels(p_szNameSpace=namespace, labels=label_dict)

    """
    查询meta pallas pod的相关信息
    :return: 返回pod对象的list或None
    """

    def get_meta_pallas_pods(self, namespace=''):
        namespace = namespace if namespace else self.__config.get_pod_namespace()
        self.__log.info('namespace：%s' % namespace)

        # 获取label信息
        label_dict = PodLabel.get_labels_via_name(PodLabel.META_PALLAS)

        return self.__k8s_handler.List_Pods_By_Labels(p_szNameSpace=namespace, labels=label_dict)

    """
    查询nfs server的相关信息
    :return: 返回pod对象的list或None
    """

    def get_nfs_server_pods(self, namespace=''):
        namespace = namespace if namespace else self.__config.get_pod_namespace()
        self.__log.info('namespace：%s' % namespace)

        # 获取label信息
        label_dict = PodLabel.get_labels_via_name(PodLabel.NFS)

        return self.__k8s_handler.List_Pods_By_Labels(p_szNameSpace=namespace, labels=label_dict)

    """
    查询db pallas pod的相关信息
    :return: 返回pod对象的list或None
    """

    def get_db_pallas_pods(self, namespace=''):
        namespace = namespace if namespace else self.__config.get_pod_namespace()
        self.__log.info('namespace：%s' % namespace)

        # 获取label信息
        label_dict = PodLabel.get_labels_via_name(PodLabel.PALLAS)

        return self.__k8s_handler.List_Pods_By_Labels(p_szNameSpace=namespace, labels=label_dict)

    """
    查询db worker pod的相关信息
    :return: 返回pod对象的list或None
    """

    def get_worker_pods(self, namespace=''):
        namespace = namespace if namespace else self.__config.get_pod_namespace()
        self.__log.info('namespace：%s' % namespace)

        # 获取label信息
        label_dict = PodLabel.get_labels_via_name(PodLabel.WORKER)

        return self.__k8s_handler.List_Pods_By_Labels(p_szNameSpace=namespace, labels=label_dict)

    """
    查询db executor pod的相关信息
    :return: 返回pod对象的list或None
    """

    def get_executor_pods(self, namespace=''):
        namespace = namespace if namespace else self.__config.get_pod_namespace()
        self.__log.info('namespace：%s' % namespace)

        # 获取label信息
        label_dict = PodLabel.get_labels_via_name(PodLabel.EXECUTOR)

        return self.__k8s_handler.List_Pods_By_Labels(p_szNameSpace=namespace, labels=label_dict)

    """
    根据pod名称查询pod名称的相关信息
    :param namespace: pod所在的namespace
    :param pod_name: pod的名称，必填项
    :return: 返回pod对象或None
    """

    def get_pod_by_name(self, namespace=None, pod_name=None):
        namespace = namespace if namespace else self.__config.get_pod_namespace()
        self.__log.info('namespace：%s' % namespace)

        if not pod_name:
            ExceptionHandler.raise_up('未指定pod的名称！')
            return

        all_pod_list = self.__k8s_handler.List_Pods(p_szNameSpace=namespace)

        for pod in all_pod_list:
            if pod.pod_name == pod_name:
                return pod
        return None





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
                ExceptionHandler.raise_up('没有提供pallas_name!')

            if not pallas_port:
                ExceptionHandler.raise_up('没有提供pallas_port!')

            if not pallas_path:
                ExceptionHandler.raise_up('没有提供pallas_path!')

            if not pallas_host:
                ExceptionHandler.raise_up('没有提供pallas_host!')

            sql = Sql(self.__test_env)

            #判断节点是否存在，若存在，直接报错
            #result = sql.run(
            #    "select * from information_schema.storage_nodes WHERE HOST='{0}' AND PORT='{1}'".format(pallas_host, pallas_port))
            #if result:
            #    raise Exception('pallas节点已存在，创建pallas节点失败！')

            #判断pallas节点的路径是否存在，若已存在，删除改目录
            host = RemoteHost(pallas_host, self.__config.get_authority_user(), self.__config.get_authority_pwd())
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

                    db_pod = self.get_pod_by_name(self.__config.get_pod_namespace(), pallas_name)
                    if db_pod:
                        if db_pod.pod_name == pallas_name and db_pod.pod_status == 'Running':
                            self.__log.info('成功创建db pallas节点：{0}'.format(pallas_name))
                            return True
                    if n != 4:
                        self.__log.info('sleep {0}s...'.format(n * base_second))
                        time.sleep(n * base_second)
                    n = n + 1

                self.__log.info('第{0}次验证db pallas状态'.format(n))

                db_pod = self.get_pod_by_name(self.__config.get_pod_namespace(), pallas_name)
                if db_pod:
                    if db_pod.pod_name == pallas_name and db_pod.pod_status == 'Running':
                        self.__log.info('成功创建db pallas节点：{0}'.format(pallas_name))
                        return True
                ExceptionHandler.raise_up('创建db pallas节点：{0}失败，请查看对应的pod状态！'.format(pallas_name))

            ExceptionHandler.raise_up('创建db pallas节点：{0}失败！异常信息：{1}'.format(pallas_name, result))

        except Exception as e:
            ExceptionHandler.raise_up('创建db pallas节点：{0}出现错误！，异常信息：{1}'.format(pallas_name, e))
        finally:
            sql.disconnect()

    """
    下线pallas节点
    :param pallas_name: pallas节点名称
    :param type: offline:临时下线；shutdown：永久下线
    :return: True/False
    """

    def offline_db_pallas_node(self, pallas_name=''):
        sql = None
        try:
            if not pallas_name:
                ExceptionHandler.raise_up('没有提供pallas_name!')

            sql = Sql(self.__test_env)
            self.__log.info('开始下线db pallas节点：{0}'.format(pallas_name))
            result = sql.run("ALTER PALLAS {0} SET STATE 2".format(pallas_name))

            # 若带删除pallas节点不存在，验证pallas节点是否成功
            if 'May not exist' not in result:
                base_second = 10
                n = 1
                self.__log.info('【验证新增pallas节点状态】')
                while n < 5:
                    self.__log.info('第{0}次验证db pallas状态'.format(n))

                    db_pod = self.get_pod_by_name(self.__config.get_pod_namespace(), pallas_name)
                    if not db_pod:
                        self.__log.info('成功下线db pallas节点：{0}'.format(pallas_name))
                        return True
                    if n != 4:
                        self.__log.info('sleep {0}s...'.format(n * base_second))
                        time.sleep(n * base_second)
                    n = n + 1

                ExceptionHandler.raise_up('下线db pallas节点：{0}失败！'.format(pallas_name))

        except Exception as e:
            ExceptionHandler.raise_up('临时下线db pallas节点：{0}出现错误！，异常信息：{1}'.format(pallas_name, e))
        finally:
            if sql:
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
                ExceptionHandler.raise_up('没有提供pallas_name!')
                return False

            if not host:
                ExceptionHandler.raise_up('没有提供host!')
                return False

            if not port:
                ExceptionHandler.raise_up('没有提供port!')
                return False

            sql = Sql(self.__test_env)

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

                    db_pod = self.get_pod_by_name(self.__config.get_pod_namespace(), pallas_name)
                    if not db_pod:
                        self.__log.info('成功下线db pallas节点：{0}'.format(pallas_name))
                        return True
                    if n != 4:
                        self.__log.info('sleep {0}s...'.format(n * base_second))
                        time.sleep(n * base_second)
                    n = n + 1

                ExceptionHandler.raise_up('下线db pallas节点：{0}失败！'.format(pallas_name))

            linux = RemoteHost(host, self.__config.get_authority_user(), self.__config.get_authority_pwd())
            linux.dir.delete(pallas_node[0][1])
            self.__log.info('成功删除远程机器{0}上的pallas节点{1}的本地路径{2}'.format(host, pallas_name, pallas_node[0][1]))
        except Exception as e:
            ExceptionHandler.raise_up('临时下线db pallas节点：{0}出现错误！，异常信息：{1}'.format(pallas_name, e))
        finally:
            sql.disconnect()




    """
    restart db
    :param pallas_name_list: pallas节点信息，以list格式存储pallas节点对应的pod名称
    :return: True/False
    """

    def restart_db(self, pallas_name_list=[]):
        self.stop_db()
        self.start_db(pallas_name_list)

    """
    start db
    :param pallas_name_list: pallas节点信息，以list格式存储pallas节点对应的pod名称
    :return: True/False
    """
    def start_db(self, pallas_name_list=[]):
        start_command = '{0}/ldb-k8s.sh start'.format(self.__config.get_k8s_bin_path())
        self.__log.info('开始启动DB，执行命令：{0}'.format(start_command))
        self.__shell.execShell(start_command)

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
    def stop_db(self):
        self.__shell.execShell('{0}/ldb-k8s.sh stop'.format(self.__config.get_k8s_bin_path()))

        # 1、等待pallas节点停止成功
        if not self.verify_db_pallas_stopped():
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

            nfs_pod = self.get_nfs_server_pods(namespace)
            # 若pod数量不等于1,则跳过本次验证
            if len(nfs_pod) == 1:
                # 判断pod状态是否为running
                for pod in nfs_pod:
                    if pod.pod_status == 'Running':
                        self.__log.info('nfs server启动成功')
                        return True
                    else:
                        self.__log.warning('nfs server状态不是Running！')
                        continue
            else:
                self.__log.warning('nfs server数量不为1！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        ExceptionHandler.raise_up('nfs server启动失败')

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

            db_pods = self.get_meta_pallas_pods(namespace)
            # 若pod个数不为4,则跳过本次验证
            if len(db_pods) == 4:
                # 判断pod状态是否为running
                for pod in db_pods:
                    if pod.pod_status == 'Running':
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

        ExceptionHandler.raise_up('meta pallas启动失败')

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

            db_pods = self.get_meta_server_pods(namespace)
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

        ExceptionHandler.raise_up('meta server启动失败')

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

            db_pods = self.get_db_server_pods(namespace)
            # 若pod个数不为3,则跳过本次验证
            if len(db_pods) == 3:
                # 判断pod状态是否为running
                for pod in db_pods:
                    if pod.pod_status == 'Running':
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

        ExceptionHandler.raise_up('db server启动失败')

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

            pod_list = self.get_db_pallas_pods(namespace)
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

        ExceptionHandler.raise_up('pallas节点启动失败!')

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

            app_info = self.__yarn.get_appid_by_user_and_appname(user, 'linkoopmeta-worker-DEFAULT')
            if app_info:
                if self.__yarn.get_app_state(app_info) == 'RUNNING':
                    self.__log.info('meta worker启动成功')
                    return True

            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        if n == 5:
            ExceptionHandler.raise_up('meta worker启动失败！')

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

            app_info = self.__yarn.get_appid_by_user_and_appname(user, 'linkoopdb-worker-DEFAULT')
            if app_info:
                if self.__yarn.get_app_state(app_info) == 'RUNNING':
                    self.__log.info('db worker启动成功')
                    return True

            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        if n == 5:
            ExceptionHandler.raise_up('db worker启动失败！')
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

            db_pods = self.get_meta_pallas_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('meta pallas关闭成功')
                return True

            self.__log.warning('meta pallas的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        ExceptionHandler.raise_up('meta pallas关闭失败！')

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

            db_pods = self.get_meta_server_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('meta server关闭成功')
                return True

            self.__log.warning('meta server的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        ExceptionHandler.raise_up('meta server关闭失败！')

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

            db_pods = self.get_db_server_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('db server关闭成功')
                return True

            self.__log.warning('db server的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        ExceptionHandler.raise_up('db server关闭失败！')

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

            db_pods = self.get_db_pallas_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('db pallas关闭成功')
                return True

            self.__log.warning('db pallas的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        ExceptionHandler.raise_up('db pallas关闭失败！')
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

            db_pods = self.get_nfs_server_pods(namespace)
            # 若pod个数不为0，返回False
            if len(db_pods) == 0:
                self.__log.info('nfs server关闭成功')
                return True

            self.__log.warning('nfs server的pod数量不为0！')
            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        ExceptionHandler.raise_up('nfs server关闭失败！')

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

            app_info = self.__yarn.get_appid_by_user_and_appname(user, 'linkoopmeta-worker-DEFAULT')
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

            app_info = self.__yarn.get_appid_by_user_and_appname(user, 'linkoopdb-worker-DEFAULT')
            if not app_info:
                self.__log.info('db worker关闭成功')
                return True

            self.__log.info('sleep {0}s...'.format(n * base_second))
            time.sleep(n * base_second)

        ExceptionHandler.raise_up('db worker关闭失败！')

    """
    永久删除db中所有pallas节点
    :return: None
    """
    def clean_db_pallas(self):
        sql = None
        try:

            sql = Sql()
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
            ExceptionHandler.raise_up('删除pallas节点并清空本地路径失败！异常信息：{0}'.format(e))
        finally:
            if sql:
                sql.disconnect()

class _LocalInit:
    def __init__(self, test_env='P1-K8S-ENV'):
        pass

    def start_db(self):
        pass

    def stop_db(self):
        pass

class _HostInit:
    def __init__(self, host_ip, login_user, login_password):
        self.__linux = Shell(ip=host_ip, username=login_user, password=login_password)
        pass

#_YarnInit -> begin
class _YarnInit:
    def __init__(self, host_ip='', login_user='', login_password=''):
        self.__config = ConfigUtil()
        if not host_ip or not login_user or not login_password:
            host_ip = self.__config.get_main_server_ip()
            login_user = self.__config.get_authority_user()
            login_password = self.__config.get_authority_pwd()

        self.__linux = Shell(ip=host_ip, username=login_user, password=login_password)

        self.__log = Log.MyLog()
        pass

    """
    判断yarn上指定的application ID的应用是否存在
    :param application_id: application_id
    :return: True/False
    """
    def exists(self, application_id):
        self.__log.info('根据appid判断yarn上指定的application ID的应用是否存在')
        if not application_id:
            ExceptionHandler.raise_up('未指定应用ID！')

        app_info = self.list_app()

        for (k, v) in app_info.items():
            if k == application_id:
                return True
        return False

    """
    根据User和Application-Name，获取应用ID，若存在返回应用ID，若不存在则返回false
    :param application_id: application_id
    :return: True/False
    """
    def get_appid_by_user_and_appname(self, user, app_name):
        self.__log.info('根据User和Application-Name，获取应用ID')
        app_info = self.list_app()

        for (k, v) in app_info.items():
            if '{0}|{1}'.format(app_name, user) in v:
                self.__log.info("user='{0}',app_name='{1}'对应的app_id={2}".format(user, app_name, k))
                return k
        self.__log.info("user='{0}',app_name='{1}'对应的app不存在！".format(user, app_name))
        return None

    """
    查询yarn上所有的application的Application-Id,Application-Name和User
    :return: 返回字典：Dict['{Application-Id}'] = '{Application-Name}|{User}|{State}'
    """
    def list_app(self):
        self.__log.info('【查询yarn上所有的application的Application-Id,Application-Name和User】')
        app_info = {}
        app_list = self.__linux.execShell('yarn application -list')

        for line in app_list:
            print(line)
            if 'application_' in line:
                new_line = re.sub(' +', '', line)
                strs = new_line.split('\t')
                app_info[strs[0]] = '{0}|{1}|{2}'.format(strs[1], strs[3], strs[5])

        return app_info

    """
    kill指定Application-Id的应用
    :param app_ids: List(Application-Id)
    :return: True/False
    """
    def kill_apps(self, app_ids):
        self.__log.info('【删除指定Application-Id的应用】')

        if not app_ids:
            self.__log.info('未指定应用Application-Ids！')

        #删除应用
        for app_id in app_ids:
            self.__linux.execShell('yarn application -kill {0}'.format(app_id))
        #验证删除结果，若有任一application未删除成功，返回False

        for app_id in app_ids:
            if self.exists(app_id):
                ExceptionHandler.raise_up('Failed to kill application: {0}'.format(app_id))
        return True

    """
    根据application-id返回其state
    :param app_id: Application-Id
    :return: app状态，若无此app，返回None
    """
    def get_app_state(self, app_id):
        self.__log.info('【根据application-id返回其state】')
        if not app_id:
            self.__log.info('未指定应用Application-Id！')

        app_info = self.list_app()
        if app_info:
            for (k, v) in app_info.items():
                if k == app_id:
                    return str(v).split('|')[2]
        self.__log.warning('application-id={0}的引用不存在！'.format(app_id))
        return None
#_YarnInit -> end

if __name__=="__main__":
    # sql = Sql('NODE66')
    ExceptionHandler.raise_up('test')

    linkoopdb = Linkoopdb('NODE66')
    ip = linkoopdb.k8s_model.get_db_server_pods()

    pallas_pod_list = ['node649011']
    #
    # #linkoopdb.k8s_model.start_db(pallas_name_list=pallas_pod_list)
    #
    # #linkoopdb.k8s_model.stop_db(pallas_name_list=pallas_pod_list)
    # pallas_name_list = ['node6440001', 'node6440002', 'node6440003', 'node6440004']
    # linkoopdb.k8s_model.verify_db_pallas_started(pallas_name_list)