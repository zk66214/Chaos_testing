# -*- coding:utf-8 -*-

import re
import os
from config_utils.parse_config import ConfigUtil
from common import Log
from common.LocalHost import *
from common.Shell import *
from common.Pod import *
from common.EnumPodLabel import *


class RemoteHost:
    def __init__(self, ip='', username='', password=''):
        self.__username = username
        self.__password = password
        self.__sys_ip = ip
        #self.__log = Log.MyLog()

    @property
    def yarn(self):
        return _Yarn(self.__sys_ip, self.__username, self.__password)

    @property
    def kubectl(self):
        return _Kubectl(self.__sys_ip, self.__username, self.__password)

    @property
    def dir(self):
        return _RemoteDirInit(self.__sys_ip, self.__username, self.__password)

    @property
    def file(self):
        return _RemoteFileInit(self.__sys_ip, self.__username, self.__password)

#_KubectlInit -> begin
class _Kubectl:
    def __init__(self, host_ip, login_user, login_password):
        self.__host_ip = host_ip
        self.__login_user = login_user
        self.__login_password = login_password

    @property
    def pod(self):
        return _Pod(self.__host_ip, self.__login_user, self.__login_password)

    @property
    def node(self):
        return _Node(self.__host_ip, self.__login_user, self.__login_password)

class _Pod:
    def __init__(self, host_ip='', login_user='', login_password=''):
        self.__config = ConfigUtil()
        if not host_ip:
            host_ip = self.__config.get_main_server_ip()
        if not login_user:
            login_user = self.__config.get_authority_user()
        if not login_password:
            login_password = self.__config.get_authority_pwd()

        self.__linux = Shell(ip=host_ip, username=login_user, password=login_password)
        self.__log = Log.MyLog()


    """
    根据pod名称验证pod是否存在
    :param namespace: pod所在的namespace，默认值p1
    :param label_app: app标签的内容，默认为空
    :param label_component: component标签的内容，默认为空
    :return: 返回字典集合：Dict['{pod-name}'] = '{pod-status}'
        """
    def exists(self, namespace='', pod_name=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        if not pod_name:
            self.__log.error('pod的名称未提供！')
            return

        all_pod_list = self.get_pods(namespace=namespace)

        for pod in all_pod_list:
            if pod.pod_name == pod_name:
                self.__log.info("POD - {0}存在".format(pod_name))
                return True
        self.__log.info("POD - {0}不存在".format(pod_name))
        return False

    """
    查询pod的相关信息
    :param namespace: pod所在的namespace
    :param label: 标签的内容，默认为空
    :return: 返回字典集合：List<Pod>
    """
    def get_pods(self, namespace='', label=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        pod_info_list = []
        shell_command = 'kubectl get pod -o wide'
        if namespace:
            shell_command = '{0} -n {1}'.format(shell_command, namespace)

        if label:
            shell_command = '{0} -l {1}'.format(shell_command, label)

        pod_list = self.__linux.execShell(shell_command)

        for line in pod_list:
            if not 'NAME' in line:
                new_line = re.sub(' +', '|', line)
                strs = new_line.split('|')

                pod_info = Pod()
                pod_info.pod_name = strs[0]
                pod_info.pod_status = strs[2]
                pod_info.pod_ip = strs[5]
                pod_info.pod_node = strs[6]

                pod_info_list.append(pod_info)
        return pod_info_list

    """
    查询server pod的相关信息
    :return: 返回字典集合：Dict['{pod-name}'] = '{pod-status}'
    """
    def get_server_pods(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        return self.get_pods(namespace=namespace, label=PodLabel.SERVER.value)

    """
    查询meta server pod的相关信息
    :return: 返回字典集合：Dict['{pod-name}'] = '{pod-status}'
    """
    def get_meta_server_pods(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        return self.get_pods(namespace=namespace, label=PodLabel.META_SERVER.value)

    """
    查询meta pallas pod的相关信息
    :return: 返回字典集合：Dict['{pod-name}'] = '{pod-status}'
    """
    def get_meta_pallas_pods(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        return self.get_pods(namespace=namespace, label=PodLabel.META_PALLAS.value)

    """
    查询nfs server的相关信息
    :return: 返回字典集合：Dict['{pod-name}'] = '{pod-status}'
    """
    def get_nfs_server_pods(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        return self.get_pods(namespace=namespace, label=PodLabel.NFS.value)

    """
    查询db pallas pod的相关信息
    :return: 返回字典集合：Dict['{pod-name}'] = '{pod-status}'
    """
    def get_db_pallas_pods(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        return self.get_pods(namespace=namespace, label=PodLabel.PALLAS.value)

    """
    查询db worker pod的相关信息
    :return: 返回字典集合：Dict['{pod-name}'] = '{pod-status}'
    """

    def get_worker_pods(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        return self.get_pods(namespace=namespace, label=PodLabel.WORKER.value)

    """
    查询db executor pod的相关信息
    :return: 返回字典集合：Dict['{pod-name}'] = '{pod-status}'
    """

    def get_executor_pods(self, namespace=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        return self.get_pods(namespace=namespace, label=PodLabel.EXECUTOR.value)

    """
    根据pod名称查询pod名称的相关信息
    :param namespace: pod所在的namespace，默认值p1
    :param pod_name: pod的名称，必填项
    :return: 返回字典集合：Dict['{pod-name}'] = '{pod-status}'
    """
    def get_pod_by_name(self, namespace='', pod_name=''):
        if not namespace:
            self.__log.info('没有指定namespace，使用默认值：{0}'.format(self.__config.get_pod_namespace()))
            namespace = self.__config.get_pod_namespace()

        if not pod_name:
            self.__log.error('未指定pod的名称！')
            return

        all_pod_list = self.get_pods(namespace=namespace)

        for pod in all_pod_list:
            if pod.pod_name == pod_name:
                return pod
        return None

class _Node:
    def __init__(self, host_ip, login_user, login_password):
        self.__linux = Shell(ip=host_ip, username=login_user, password=login_password)
        pass
#_KubectlInit -> end

#_YarnInit -> begin
class _Yarn:
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
            self.__log.error('未指定应用ID！')

        app_info = self.list_app()

        for (k, v) in app_info.items():
            if k == application_id:
                return True
        return False

    """
        判断yarn上指定的application ID的应用是否存在
        :param application_id: application_id
        :return: True/False
        """

    def exists(self, user='', app_name=''):
        self.__log.info('根据user和appname判断yarn上指定的application ID的应用是否存在')
        if not user:
            self.__log.error('未指定应用user！')

        if not app_name:
            self.__log.error('未指定应用app_name！')

        app_info = self.list_app()

        for (k, v) in app_info.items():
            if user in v and app_name in v:
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
        kill_success = True
        for app_id in app_ids:
            if self.exists(app_id):
                self.__log.error('Failed to kill application: {0}'.format(app_id))
                kill_success = False
        return kill_success

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

# RemoteDir -> begin
class _RemoteDirInit:
    def __init__(self, remote_ip='', username='', password=''):
        self.__config = ConfigUtil()
        if not remote_ip:
            print('远程机器的IP信息缺失！')
            raise Exception('远程机器的IP信息缺失！')

        if username:
            self.__username = username
        else:
            self.__username = self.__config.get_authority_user()

        if password:
            self.__password = password
        else:
            self.__password = self.__config.get_authority_pwd()

        self.__remote_ip = remote_ip

        self.__linux = Shell(ip=self.__remote_ip, username=self.__username, password=self.__password)
        self.__log = Log.MyLog()

    """
    判断文件夹是否存在
    :return: True/False
    """

    def exists(self, remote):
        return True if self.__linux.execShell("[ -d '{0}' ] && echo 1 || echo 0".format(remote))[
                           0].strip() == "1" else False

    """
    判断是否是目录
    :return: true/false
    """

    def isDir(self, remote):
        return True if self.__linux.execShell("[ -d '{0}' ] && echo 1 || echo 0".format(remote))[
                           0].strip() == "1" else False
    """
    新建文件夹，若该文件夹已存在，则跳过
    :param new_file: 文件路径，不能为空
    :return: None
    """

    def create(self, remote_dir):

        if self.exists(remote_dir):
            self.__log.info("文件夹{0}已存在！".format(remote_dir))
            return True

        self.__linux.execShell("mkdir {0}".format(remote_dir))

        if not self.exists(remote_dir):
            self.__log.error("新建文件夹{0}失败！".format(remote_dir))
            return False

        self.__log.info("新建文件夹{0}成功！".format(remote_dir))
        return True

    """
    删除远程机器上的文件夹
    :param remote_dir: 文件夹路径
    :return: None
    """

    def delete(self, remote_dir):
        if not self.exists(remote_dir):
            self.__log.warning("文件夹{0}不存在！".format(remote_dir))
            return True

        self.__linux.execShell("rm -rf {0}".format(remote_dir))

        if self.exists(remote_dir):
            self.__log.error("删除文件夹{0}失败！".format(remote_dir))
            return False

        self.__log.info("删除文件夹{0}成功！".format(remote_dir))
        return True
# RemoteDir -> end

# RemoteFile -> begin
class _RemoteFileInit():
    def __init__(self, remote_ip='', username='', password=''):
        self.__config = ConfigUtil()
        if not remote_ip:
            print('远程机器的IP信息缺失！')
            raise Exception('远程机器的IP信息缺失！')

        if username:
            self.__username = username
        else:
            self.__username = self.__config.get_authority_user()

        if password:
            self.__password = password
        else:
            self.__password = self.__config.get_authority_pwd()

        self.__remote_ip = remote_ip

        self.__linux = Shell(ip=self.__remote_ip, username=self.__username, password=self.__password)

        self.__ssh = self.__linux.get_ssh()

        self.__sftp = self.__ssh.open_sftp()

        self.__log = Log.MyLog()
        self.__localMachine = LocalHost()

        self.__work_folder = self.__localMachine.Dir.get_work_folder()

        if not os.path.exists(self.__work_folder):
            os.mkdir(self.__work_folder)

    """
    判断文件是否存在
    :return: true/false
    """

    def exists(self, remote_file):
        return True if self.__linux.execShell("[ -e '{0}' ] && echo 1 || echo 0".format(remote_file))[
                           0].strip() == "1" else False

    """
    判断是否是目录
    :return: true/false
    """

    def isFile(self, remote_file):
        return True if self.__linux.execShell("[ -f '{0}' ] && echo 1 || echo 0".format(remote_file))[
                           0].strip() == "1" else False

    """
    新建文件并添加内容，若该文件已存在，则删除后重新创建
    :param new_file: 文件路径，不能为空
    :param content: 文件内容，可为空
    :return: True/False
    """

    def create(self, remote_file, content=""):
        try:
            if not os.path.exists(self.__work_folder):
                os.mkdir(self.__work_folder)

            if self.exists(remote_file):
                self.__log.warning('文件{0}已存在!'.format(remote_file))
                return False

            self.__linux.execShell("touch {0}".format(remote_file))

            self.append(remote_file, content)
            self.__log.info("新建文件{0}成功！".format(remote_file))
            return True
        except Exception as e:
            self.__log.error('新建远程文件失败，异常信息:', e)
            return False

    """
    批量替换文件中指定的字符串
    :param file: 远程文件路径
    :param update_content: 需要替换的字符串，类型为字典，key为需要替换的字符串，value为替换的字符串
    :return: True/False
    """

    def update(self, remote_file, update_content):
        try:
            if not os.path.exists(self.__work_folder):
                os.mkdir(self.__work_folder)

            if not self.exists(remote_file):
                self.__log.warning('文件{0}不存在!'.format(remote_file))
                return False
            local_file = self.__work_folder + os.path.basename(remote_file)

            self.get_file_from_remote(remote_file, local_file)

            self.__localMachine.File.modify_file(local_file, update_content)

            self.put_file_to_remote(local_file, remote_file)
            self.__log.info('更新远程文件{0}成功'.format(remote_file))
            return True
        except Exception as e:
            self.__log.error('更新远程文件失败，异常信息:', e)
            return False

    """
    在文件后追加内容
    :param file: 远程文件路径
    :param update_content: 需要追加的字符串
    :return: True/False
    """

    def append(self, remote_file, append_content=""):
        try:
            if not os.path.exists(self.__work_folder):
                os.mkdir(self.__work_folder)

            if not self.exists(remote_file):
                self.__log.warning('文件{0}不存在!'.format(remote_file))
                return False
            local_file = self.__work_folder + os.path.basename(remote_file)

            self.get_file_from_remote(remote_file, local_file)

            with open(local_file, 'a+') as f:
                f.write(append_content)

            self.put_file_to_remote(local_file, remote_file)
            self.__log.info('更新远程文件{0}成功'.format(remote_file))
            return True
        except Exception as e:
            self.__log.error('更新远程文件失败，异常信息:', e)
            return False

    """
    删除远程机器上的文件
    :param remote_file: 文件绝对路径
    :return: True/False
    """

    def delete(self, remote_file):
        self.__linux.execShell('rm -f {0}'.format(remote_file))

        if self.exists(remote_file):
            self.__log.error('删除文件{0}失败！'.format(remote_file))
            return False

        self.__log.info('成功删除文件{0}！'.format(remote_file))
        return False

    """
    拷贝本地文件到远程机器
    :param local_file: 本地文件绝对路径
    :param remote_file: 远程机器文件绝对路径
    :return: None
    """

    def put_file_to_remote(self, local, remote):
        try:
            # 本地文件路径不存在，抛出异常
            if not os.path.exists(local):
                raise Exception('本地文件路径{0}不存在!'.format(local))

            if os.path.isdir(local):  # 判断本地参数是目录
                for f in os.listdir(local):  # 遍历本地目录
                    # 服务器文件已存在，则先删除
                    if self.exists(os.path.join(remote + f)):
                        self.delete(os.path.join(remote + f))
                    if not os.path.isdir(os.path.join(local + f)):
                        self.__sftp.put(os.path.join(local + f), os.path.join(remote + f))  # 上传目录中的文件
                        self.__log.info('成功拷贝本地文件{1}到远程机器{0}:{2}'.format(self.__remote_ip, os.path.join(local + f),
                                                                         os.path.join(remote + f)))
            elif os.path.isfile(local):  # 判断本地参数是文件
                # 服务器文件已存在，则先删除
                if self.exists(remote):
                    self.delete(remote)

                self.__sftp.put(local, remote)  # 上传文件
                self.__log.info('成功拷贝本地文件{1}到远程机器{0}:{2}'.format(self.__remote_ip, local, remote))
            else:
                self.__log.error('文件路径{0}不存在!'.format(remote))
                return

        except Exception as e:
            self.__log.error('拷贝本地文件{1}到远程机器{0}:{2}失败！异常信息：{4}'.format(self.__remote_ip, local, remote, str(e)))

    """
    拷贝远程机器文件到本地
    :param local: 本地文件绝对路径
    :param remote: 远程机器文件绝对路径
    :return: None
    """

    def get_file_from_remote(self, remote='', local=''):
        try:
            if not remote or not local:
                self.__log.error('remote或local不能为空!')
                return

            if not self.exists(remote):
                self.__log.error('远程机器{0}不存在!'.format(remote))
                return

            # 本地文件所在目录不存在，创建目录
            if not os.path.exists(os.path.dirname(local)):
                os.makedirs(os.path.dirname(local))

            if self.isFile(remote):  # 判断remote是文件
                # 本地文件已存在，则先删除
                if os.path.exists(local):
                    os.remove(local)

                self.__sftp.get(remote, local)  # 下载文件
                self.__log.info('成功拷贝远程文件{0}:{2}到本地目录{1}'.format(self.__remote_ip, local, remote))
            elif self.exists(remote):  # 判断remote是目录
                for f in self.__sftp.listdir(remote):  # 遍历远程目录
                    if not self.exists(os.path.join(remote + f)):
                        self.__sftp.get(os.path.join(remote + f), os.path.join(local + f))  # 下载目录中文件
                        self.__log.info('成功拷贝远程文件{0}:{2}到本地目录{1}'.format(self.__remote_ip, os.path.join(local + f),
                                                                         os.path.join(remote + f)))
        except Exception as e:
            self.__log.error('拷贝远程文件{0}:{2}到本地目录{1}失败，异常信息：{3}'.format(self.__remote_ip, remote,local, str(e)))

    """
    查找包含key的行，替换为new_str
    :param file: 原文件路径
    :param key: 寻找修改行的关键字符
    :param new_str: 替换旧行的新内容
    :return: None
    """

    def replace_line_with_new_str(self, remote_file, key, new_str):
        try:
            #获得文件名称
            file_name = os.path.basename(remote_file)
            local_file = os.path.join(self.__work_folder, file_name)
            #将远程文件copy到本地work目录
            self.get_file_from_remote(remote_file, local_file)

            #修改本地文件内容
            self.__localMachine.File.replace_line_with_new_str(os.path.join(self.__work_folder,file_name), key, new_str)

            #将远程机器上需要修改的文件替换为本地文件
            self.put_file_to_remote(local_file, remote_file)

            self.__log.info('替换文件{0}下包含关键字符{1}的行内容成功'.format(remote_file, key))
        except Exception as e:
            self.__log.error('替换文件{0}下包含关键字符{1}的行内容失败！异常信息：{2}'.format(remote_file, key, str(e)))
        return None
# RemoteFile -> end

if __name__=="__main__":
    cmds = "pwd"
    config_info = ConfigUtil()
    #host_ips = config_info.get_host_ips().split(',')
    host_ips = config_info.get_main_server_ip()
    linux = Shell(config_info.get_main_server_ip(), config_info.get_authority_user(), config_info.get_authority_pwd())
    linux.execShell('pwd')
    # linux.kubectl.pod.get_meta_pallas_pods()
    # linux.kubectl.pod.get_pods()
    # linux.yarn.list_app()