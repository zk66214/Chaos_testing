from kubernetes import client
from kubernetes.client.rest import ApiException
import urllib3
import sys
import re

class LDB_ChaosTestException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

class LDB_ChaosTest():
    def __init__(
            self,
            p_APIServer,
            p_Token):
        self.APIServer = p_APIServer
        self.Token = p_Token

        # disable security warning
        urllib3.disable_warnings()

        # Create a configuration object
        configuration = client.Configuration()

        # Specify the endpoint of your Kube cluster
        configuration.host = self.APIServer

        # Security part.
        # In this simple example we are not going to verify the SSL certificate of
        # the remote cluster (for simplicity reason)
        configuration.verify_ssl = False

        # Nevertheless if you want to do it you can with these 2 parameters
        # configuration.verify_ssl=True
        # ssl_ca_cert is the filepath to the file that contains the certificate.
        # configuration.ssl_ca_cert="certificate"
        configuration.api_key = {"authorization": "Bearer " + self.Token}

        # configuration.api_key["authorization"] = "bearer " + Token
        # configuration.api_key_prefix['authorization'] = 'Bearer'
        # configuration.ssl_ca_cert = 'ca.crt'
        # Create a ApiClient with our config
        client.Configuration.set_default(configuration)

        # 标记K8S已经连接
        self.Connected = True

    CHAOS_COMMANDS = \
        {'pod_failure':
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
            }
        }

    def List_Pods(self, p_szNameSpace=None):
        APIHandler = client.CoreV1Api()
        ret = APIHandler.list_pod_for_all_namespaces(watch=False)
        m_Pods = []
        for i in ret.items:
            if p_szNameSpace is not None:
                if (i.metadata.namespace == p_szNameSpace):
                    m_Pods.append(
                        {
                            "pod_ip": i.status.pod_ip,
                            "namespace": i.metadata.namespace,
                            "name": i.metadata.name,
                        })
            else:
                m_Pods.append(
                    {
                        "pod_ip": i.status.pod_ip,
                        "namespace": i.metadata.namespace,
                        "name": i.metadata.name,
                    })
        return m_Pods

    def UndoKubectlCommand(self, p_szJsonCommmand):
        k8s_custom_api = client.CustomObjectsApi()
        m_Group = p_szJsonCommmand["apiVersion"].split('/')[0]
        m_Version = p_szJsonCommmand["apiVersion"].split('/')[1]
        m_NameSpace = p_szJsonCommmand["metadata"]["namespace"]
        m_Plural = p_szJsonCommmand["kind"].lower()
        m_Name = p_szJsonCommmand["metadata"]["name"]

        try:
            resp = k8s_custom_api.delete_namespaced_custom_object(
                namespace=m_NameSpace,
                version=m_Version,
                group=m_Group,
                plural=m_Plural,
                name=m_Name,
                body=client.V1DeleteOptions())
        except ApiException as ae:
            raise LDB_ChaosTestException(str(ae.body))
        return True

    def DoKubectlCommand(self, p_szJsonCommmand):
        k8s_custom_api = client.CustomObjectsApi()
        m_Group = p_szJsonCommmand["apiVersion"].split('/')[0]
        m_Version = p_szJsonCommmand["apiVersion"].split('/')[1]
        m_NameSpace = p_szJsonCommmand["metadata"]["namespace"]
        m_Plural = p_szJsonCommmand["kind"].lower()

        try:
            resp = k8s_custom_api.create_namespaced_custom_object(
                body=p_szJsonCommmand,
                namespace=m_NameSpace,
                version=m_Version,
                group=m_Group,
                plural=m_Plural)
        except ApiException as ae:
            raise LDB_ChaosTestException(str(ae.body))
        return True

    def disable_pod_failure(self, p_szPodGroups, p_szNameSpace):
        m_JsonCommand = self.CHAOS_COMMANDS['pod_failure']
        m_Pods = self.List_Pods(p_szNameSpace=p_szNameSpace)
        m_TargetPos = []
        for m_Pod in m_Pods:
            if re.match(p_szPodGroups, m_Pod['name']):
                m_TargetPos.append(m_Pod['name'])
        m_JsonCommand['spec']['selector']['pods'][p_szNameSpace] = m_TargetPos

        return self.UndoKubectlCommand(m_JsonCommand)

    def enable_pod_failure(self, p_szPodGroups, p_szNameSpace):
        m_JsonCommand = self.CHAOS_COMMANDS['pod_failure']
        m_Pods = self.List_Pods(p_szNameSpace=p_szNameSpace)
        m_TargetPos = []
        for m_Pod in m_Pods:
            if re.match(p_szPodGroups, m_Pod['name']):
                m_TargetPos.append(m_Pod['name'])
        m_JsonCommand['spec']['selector']['pods'][p_szNameSpace] = m_TargetPos

        return self.DoKubectlCommand(m_JsonCommand)


if __name__ == '__main__':
    '''
    APISERVER=$(kubectl config view --minify | grep server | cut -f 2- -d ":" | tr -d " ")
    echo "APISERVER = '"$APISERVER"'"
    Token=$(kubectl describe secret $(kubectl get secret -n kube-system | grep ^admin-user | awk '{print $1}') -n kube-system | grep -E '^token'| awk '{print $2}')
    echo "Token = '"$Token"'"
    '''
    APISERVER = 'https://192.168.1.64:6443'
    Token = "eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJhZG1pbi11c2VyLXRva2VuLXdkdGY5Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImFkbWluLXVzZXIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiJmNTQ2MTJlNS1kZTE0LTExZWEtYjE1NC1kY2Y0MDFlNjgzMTgiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZS1zeXN0ZW06YWRtaW4tdXNlciJ9.FlGESV2KGiV7_So5YI4vHIwmQ7cwNy7SyTHoKPR_kPpnEL2T6bajdfr4GWHhanGTM7wWIzjHiqRSbhWpdepkEphJNkJ4gHxtxNvo5BkJDEJ_CM9iEbCSjbY3Q-Z1KDVFBjcJUo0zxlJT0VArJBWmBhq-k3s-9Cy3KwOdd7bEqN7kn6ozzmmGECeVbZg2Vq9epTmKDrbazZ_kFErm6EGOj41LZ_25f1vWtbKRBMt2ziajc8wqSxccEn9qglpAR4WbcVapK2lKJ5hWQYOsimhYReEwJhoBxIengP5w_tk0K90NgefKnxn9GnEHv4lHTaKuZWu2kPzSxQjGNPpG8yTesQ"

    m_ChaoTest = LDB_ChaosTest(p_APIServer=APISERVER, p_Token=Token)
    # m_ChaoTest.enable_pod_failure("busybox.*", 'ldb-test')
    m_ChaoTest.disable_pod_failure("busybox.*", 'ldb-test')
