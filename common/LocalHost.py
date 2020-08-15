# -*- coding:utf-8 -*-
import os
import stat
import shutil
from common import Log


class LocalHost:
    def __init__(self):
        self.__log = Log.MyLog()

    @property
    def Dir(self):
        return _DirInit()

    @property
    def File(self):
        return _FilelInit()


class _DirInit:
    def __init__(self):
        pass

    def get_work_folder(self):
        log = Log.MyLog()

        worker_folder_path = os.path.abspath(os.path.dirname(__file__)).split('/common')[0] + "/work_folder/"

        if not os.path.exists(worker_folder_path):
            try:
                os.mkdir(worker_folder_path)
            except Exception as e:
                log.error('获取本地work folder路径失败！失败原因：{0}'.format(e))
                return None

        log.info('成功获取本地路径：work folder={0}'.format(worker_folder_path))
        return worker_folder_path

    def get_conf_folder(self):
        log = Log.MyLog()

        conf_path = os.path.abspath(os.path.dirname(__file__)).split('/common')[0] + "/conf/"

        if not os.path.exists(conf_path):
            log.error('获取本地conf路径失败！')
            return None

        log.info('成功获取本地路径：conf={0}'.format(conf_path))
        return conf_path

    def get_test_config_folder(self):
        log = Log.MyLog()

        test_config_path = os.path.abspath(os.path.dirname(__file__)).split('/common')[0] + "/test/config/"

        if not os.path.exists(test_config_path):
            log.error('获取本地test config路径失败！')
            return None

        log.info('成功获取本地路径：test config={0}'.format(test_config_path))
        return test_config_path

class _FilelInit:
    def __init__(self):
        pass
    """
    替换文件中的字符串
    :param file:文件全路径
    :param update_content: [key=原始字符,value=新字符]
    :return:
    """

    def modify_file(self, file, update_content):
        log = Log.MyLog()
        file_data = ""
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                for (k, v) in update_content.items():
                    if k in line:
                        line = line.replace(k, v)
                file_data += line
        with open(file, "w", encoding="utf-8") as f:
            f.write(file_data)
        log.info('修改文件{0}内容完成'.format(file))

    """
    将替换的字符串写到一个新的文件中，然后将原文件删除，新文件改为原来文件的名字
    :param old_file: 原文件路径
    :param new_file: 新文件路径
    :param update_content: 需要替换的字符串，类型为字典，key为需要替换的字符串，value为替换的字符串
    :return: None
    """

    def modify_and_create_file(self, old_file, new_file, update_content):
        log = Log.MyLog()
        with open(old_file, "r", encoding="utf-8") as f1, open(new_file, "w", encoding="utf-8") as f2:
            for line in f1:
                for (k, v) in update_content.items():
                    if k in line:
                        line = line.replace(k, v)
                f2.write(line)
            log.info('修改文件{0}内容完成，生成新的文件{1}'.format(old_file, new_file))

    """
    将替换的字符串写到一个新的文件中，然后将原文件删除，新文件改为原来文件的名字
    :param old_file: 原文件路径
    :param new_file: 新文件路径
    :param update_content: 需要替换的字符串，类型为字典，key为需要替换的字符串，value为替换的字符串
    :return: None
    """

    def copy_file(self, old_file, new_file):
        log = Log.MyLog()
        with open(old_file, "r", encoding="utf-8") as f1, open(new_file, "w", encoding="utf-8") as f2:
            for line in f1:
                f2.write(line)
            log.info('复制文件{0}内容完成，生成新的文件{1}'.format(old_file, new_file))
    """
    删除本地目录下的所有文件以及文件夹下的文件
    :param file_path: 目录
    :return: True/False
    """

    def delete_file_in_folder(self, file_path):
        log = Log.MyLog()
        if os.path.exists(file_path):
            for fileList in os.walk(file_path):
                for name in fileList[2]:
                    os.chmod(os.path.join(fileList[0], name), stat.S_IWRITE)
                    os.remove(os.path.join(fileList[0], name))
            shutil.rmtree(file_path)
            log.info('成功删除本地文件夹{0}下的文件'.format(file_path))
            return True
        else:
            log.warning('删除本地文件夹{0}下的文件失败！'.format(file_path))
            return False

    """
    查找包含key的行，替换为new_str
    :param file: 原文件路径
    :param key: 寻找修改行的关键字符
    :param new_str: 替换旧行的新内容
    :return: None
    """

    def replace_line_with_new_str(self, file, key, new_str):
        try:
            log = Log.MyLog()
            with open(file, 'r', encoding='utf-8') as f:
                lines = []  # 创建了一个空列表，里面没有元素
                for line in f.readlines():
                    if line != '\n':
                        lines.append(line)
                f.close()
            with open(file, 'w', encoding='utf-8') as f:
                for line in lines:
                    if key in line:
                        line = new_str
                        f.write('%s\n' % line)
                    else:
                        f.write('%s' % line)
            log.info('替换文件{0}下包含关键字符{1}的行内容成功'.format(file, key))
        except Exception as e:
            log.error('替换文件{0}下包含关键字符{1}的行内容失败！异常信息：{2}'.format(file, key,str(e)))