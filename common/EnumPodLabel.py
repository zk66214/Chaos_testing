from enum import Enum

class Label(Enum):
    SERVER = 'app.kubernetes.io/component=database'
    META_SERVER = ''
    NFS = 'app.kubernetes.io/component=nfs'
    WORKER = 'spark-role=driver'
    EXECUTOR = 'spark-role=executor'
    PALLAS = ''

if __name__=="__main__":
    a = Label.EXECUTOR.name
    b = Label.EXECUTOR.value
    pass
