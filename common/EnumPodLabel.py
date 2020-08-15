from enum import Enum

class PodLabel(Enum):
    SERVER = 'app.kubernetes.io/component=database'
    PALLAS = ''
    META_SERVER = ''
    META_PALLAS = ''
    NFS = 'app.kubernetes.io/component=nfs'
    WORKER = 'spark-role=driver'
    EXECUTOR = 'spark-role=executor'


if __name__=="__main__":
    a = PodLabel.EXECUTOR.name
    b = PodLabel.EXECUTOR.value
    pass
