# -*- coding:utf-8 -*-

import sys
import re
import time
from common.K8SHandler import K8SHandler, K8SHandlerException


class LDB_ChaosTestException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

class LDB_ChaosTest():
    def __init__(self):
        self.m_K8SHandler = None

    def ConnectK8S(self, p_APIServer, p_Token):
        self.m_K8SHandler = K8SHandler(p_APIServer, p_Token)

    CHAOS_COMMANDS = \
        {
            'pod_failure':
            {
                "apiVersion": "chaos-mesh.org/v1alpha1",
                "kind": "PodChaos",
                "metadata": {
                    "name": "pod-failure-example",
                    "namespace": "chaos-testing"
                },
                "spec": {
                    "action": "pod-failure",
                    "mode": "one",
                    "value": "",
                    "duration": "30s",
                    "selector": {
                        "pods": {
                            "ldb-test": [
                                "linkoopdb-database-2"
                                ]
                        }
                    },
                    "scheduler": {
                        "cron": "@every 2m"
                    }
                }
            },
            'pod_kill':
            {
                "apiVersion": "chaos-mesh.org/v1alpha1",
                "kind": "PodChaos",
                "metadata": {
                    "name": "pod-kill-example",
                    "namespace": "chaos-testing"
                },
                "spec": {
                    "action": "pod-kill",
                    "mode": "one",
                    "selector": {
                        "pods": {
                            "ldb-test": [
                                "linkoopdb-database-2"
                                ]
                        }
                    },
                    "scheduler": {
                        "cron": "@every 1m"
                    }
                }
            }
        }

    def disable_pod_failure(self, p_szPodGroups, p_szNameSpace):
        m_JsonCommand = self.CHAOS_COMMANDS['pod_failure']
        m_Pods = self.m_K8SHandler.List_Pods(p_szNameSpace=p_szNameSpace)
        m_TargetPods = []
        for m_Pod in m_Pods:
            if re.match(p_szPodGroups, m_Pod.pod_name):
                m_TargetPods.append(m_Pod.pod_name)
        m_JsonCommand['spec']['selector']['pods'][p_szNameSpace] = m_TargetPods

        return self.m_K8SHandler.UndoKubectlCommand(m_JsonCommand)

    def pod_kill(self, p_szPodGroups, p_szNameSpace):
        m_JsonCommand = self.CHAOS_COMMANDS['pod_kill']
        m_Pods = self.m_K8SHandler.List_Pods(p_szNameSpace=p_szNameSpace)
        m_TargetPods = []
        for m_Pod in m_Pods:
            if re.match(p_szPodGroups, m_Pod.pod_name):
                m_TargetPods.append(m_Pod.pod_name)
        m_JsonCommand['spec']['selector']['pods'][p_szNameSpace] = m_TargetPods
        # 杀掉进程后，休息2S，随后取消掉杀进程的配置
        resp = self.m_K8SHandler.DoKubectlCommand(m_JsonCommand)
        print("resp = [" + str(resp) + "]")
        time.sleep(2)
        resp = self.m_K8SHandler.UndoKubectlCommand(m_JsonCommand)
        print("resp = [" + str(resp) + "]")

    def enable_pod_failure(self, p_szPodGroups, p_szNameSpace):
        m_JsonCommand = self.CHAOS_COMMANDS['pod_failure']
        m_Pods = self.m_K8SHandler.List_Pods(p_szNameSpace=p_szNameSpace)
        m_TargetPods = []
        for m_Pod in m_Pods:
            if re.match(p_szPodGroups, m_Pod.pod_name):
                m_TargetPods.append(m_Pod.pod_name)
        m_JsonCommand['spec']['selector']['pods'][p_szNameSpace] = m_TargetPods

        return self.m_K8SHandler.DoKubectlCommand(m_JsonCommand)


if __name__ == '__main__':
    '''
    APISERVER=$(kubectl config view --minify | grep server | cut -f 2- -d ":" | tr -d " ")
    echo "APISERVER = '"$APISERVER"'"
    Token=$(kubectl describe secret $(kubectl get secret -n kube-system | grep ^admin-user | awk '{print $1}') -n kube-system | grep -E '^token'| awk '{print $2}')
    echo "Token = '"$Token"'"
    '''
    APISERVER = 'https://192.168.1.64:6443'
    Token = "eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJhZG1pbi11c2VyLXRva2VuLXdkdGY5Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImFkbWluLXVzZXIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiJmNTQ2MTJlNS1kZTE0LTExZWEtYjE1NC1kY2Y0MDFlNjgzMTgiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZS1zeXN0ZW06YWRtaW4tdXNlciJ9.FlGESV2KGiV7_So5YI4vHIwmQ7cwNy7SyTHoKPR_kPpnEL2T6bajdfr4GWHhanGTM7wWIzjHiqRSbhWpdepkEphJNkJ4gHxtxNvo5BkJDEJ_CM9iEbCSjbY3Q-Z1KDVFBjcJUo0zxlJT0VArJBWmBhq-k3s-9Cy3KwOdd7bEqN7kn6ozzmmGECeVbZg2Vq9epTmKDrbazZ_kFErm6EGOj41LZ_25f1vWtbKRBMt2ziajc8wqSxccEn9qglpAR4WbcVapK2lKJ5hWQYOsimhYReEwJhoBxIengP5w_tk0K90NgefKnxn9GnEHv4lHTaKuZWu2kPzSxQjGNPpG8yTesQ"

    m_ChaoTest = LDB_ChaosTest()
    m_ChaoTest.ConnectK8S(p_APIServer=APISERVER, p_Token=Token)
    # m_ChaoTest.enable_pod_failure("busybox.*", 'ldb-test')
    # m_ChaoTest.disable_pod_failure("busybox.*", 'ldb-test')
    m_ChaoTest.pod_kill("busybox.*", 'ldb-test')


