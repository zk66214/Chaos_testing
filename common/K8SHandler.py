# -*- coding:utf-8 -*-

from kubernetes import client
from kubernetes.client.rest import ApiException
import urllib3
import sys
import re
import copy
import json
from common.Pod import Pod
from common.K8SNode import K8SNode
from common.K8SCustomObject import K8SCustomObject

class K8SHandlerException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class K8SHandler():
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

    def List_Nodes(self):
        APIHandler = client.CoreV1Api()
        ret = APIHandler.list_node()
        m_Nodes = []

        for i in ret.items:
            m_Node = K8SNode()
            m_Node.node_name = i.metadata.name
            m_Node.node_labels = i.metadata.labels
            m_Nodes.append(copy.copy(m_Node))
        return m_Nodes

    def List_Pods(self, p_szNameSpace=None):
        APIHandler = client.CoreV1Api()
        ret = APIHandler.list_pod_for_all_namespaces(watch=False)
        m_Pods = []

        for i in ret.items:
            if p_szNameSpace is not None:
                if i.metadata.namespace == p_szNameSpace:
                    m_Pod = Pod()
                    m_Pod.pod_name = i.metadata.name
                    m_Pod.pod_ip = i.status.pod_ip
                    m_Pod.pod_status = i.status.phase
                    m_Pod.pod_node = i.spec.node_name
                    m_Pod.pod_labels = i.metadata.labels
                    m_Pods.append(copy.copy(m_Pod))
            else:
                m_Pod = Pod()
                m_Pod.pod_name = i.metadata.name
                m_Pod.pod_ip = i.status.pod_ip
                m_Pod.pod_status = i.status.phase
                m_Pod.pod_node = i.spec.node_name
                m_Pod.pod_labels = i.metadata.labels
                m_Pods.append(copy.copy(m_Pod))
        return m_Pods

    def List_Pods_By_Labels(self, p_szNameSpace=None, labels=None):
        r_pods = None

        m_Pods = self.List_Pods(p_szNameSpace)

        for pod in m_Pods:
            matchNum = 0
            for k1, v1 in labels.items():
                for k2, v2 in pod.pod_labels.items():
                    if k1 == k2 and v1 == v2:
                        matchNum = matchNum + 1
                        break
            if matchNum == len(labels):
                r_pods.append(pod)

        return r_pods

    def DeleteCustomObjectFromJson(self, p_szJsonCommmand):
        k8s_custom_api = client.CustomObjectsApi()
        m_Group = p_szJsonCommmand["apiVersion"].split('/')[0]
        m_Version = p_szJsonCommmand["apiVersion"].split('/')[1]
        m_NameSpace = p_szJsonCommmand["metadata"]["namespace"]
        m_Plural = p_szJsonCommmand["kind"].lower()
        m_Name = p_szJsonCommmand["metadata"]["name"]

        try:
            k8s_custom_api.delete_namespaced_custom_object(
                namespace=m_NameSpace,
                version=m_Version,
                group=m_Group,
                plural=m_Plural,
                name=m_Name,
                body=client.V1DeleteOptions())
        except ApiException as ae:
            raise K8SHandlerException(str(ae.body))
        return True

    def DeleteCustomObject(self, p_Group, p_Version, p_Plural, p_NameSpace, p_Name):
        k8s_custom_api = client.CustomObjectsApi()
        try:
            k8s_custom_api.delete_namespaced_custom_object(
                namespace=p_NameSpace,
                version=p_Version,
                group=p_Group,
                plural=p_Plural,
                name=p_Name,
                body=client.V1DeleteOptions())
        except ApiException as ae:
            raise K8SHandlerException(str(ae.body))
        return True

    def ApplyCustomObjectFromJson(self, p_szJsonCommmand):
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
            raise K8SHandlerException(str(ae.body))
        return True

    def DescribeCustomObject(self, p_Group, p_Version, p_Plural, p_NameSpace):
        k8s_custom_api = client.CustomObjectsApi()
        ret = k8s_custom_api.list_namespaced_custom_object(
            namespace=p_NameSpace,
            version=p_Version,
            group=p_Group,
            plural=p_Plural)

        m_K8SCustomObjects = []
        for i in ret["items"]:
            m_K8SCustomObject = K8SCustomObject()
            m_K8SCustomObject.name = str(i['metadata']['name'])
            m_K8SCustomObject.namespace = p_NameSpace
            m_K8SCustomObject.group = p_Group
            m_K8SCustomObject.version = p_Version
            m_K8SCustomObject.plural = p_Plural
            m_K8SCustomObjects.append(copy.copy(m_K8SCustomObject))
        return m_K8SCustomObjects


if __name__ == '__main__':
    '''
    APISERVER=$(kubectl config view --minify | grep server | cut -f 2- -d ":" | tr -d " ")
    echo "APISERVER = '"$APISERVER"'"
    Token=$(kubectl describe secret $(kubectl get secret -n kube-system | grep ^admin-user | awk '{print $1}') -n kube-system | grep -E '^token'| awk '{print $2}')
    echo "Token = '"$Token"'"
    '''
    APISERVER = 'https://192.168.1.64:6443'
    Token = "eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJhZG1pbi11c2VyLXRva2VuLXdkdGY5Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImFkbWluLXVzZXIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiJmNTQ2MTJlNS1kZTE0LTExZWEtYjE1NC1kY2Y0MDFlNjgzMTgiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZS1zeXN0ZW06YWRtaW4tdXNlciJ9.FlGESV2KGiV7_So5YI4vHIwmQ7cwNy7SyTHoKPR_kPpnEL2T6bajdfr4GWHhanGTM7wWIzjHiqRSbhWpdepkEphJNkJ4gHxtxNvo5BkJDEJ_CM9iEbCSjbY3Q-Z1KDVFBjcJUo0zxlJT0VArJBWmBhq-k3s-9Cy3KwOdd7bEqN7kn6ozzmmGECeVbZg2Vq9epTmKDrbazZ_kFErm6EGOj41LZ_25f1vWtbKRBMt2ziajc8wqSxccEn9qglpAR4WbcVapK2lKJ5hWQYOsimhYReEwJhoBxIengP5w_tk0K90NgefKnxn9GnEHv4lHTaKuZWu2kPzSxQjGNPpG8yTesQ"

    m_K8SHandler = K8SHandler(p_APIServer=APISERVER, p_Token=Token)

    '''
    for row in (m_K8SHandler.List_Pods()):
        print("Name = " + row.pod_name)
        print("pod_id = " + row.pod_ip)
        print("Name = " + row.pod_node)
        print("Status = " + row.pod_status)
        print("Labels = " + str(row.pod_labels))

    for row in (m_K8SHandler.List_Nodes()):
        print("Name = " + row.node_name)
        print("Labels = " + str(row.node_labels))
    '''

    m_K8SHandler.DescribeCustomObject(p_Group='chaos-mesh.org', p_Version='v1alpha1', p_Plural='networkchaos', p_NameSpace='ldb-test')
