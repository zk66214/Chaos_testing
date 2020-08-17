from enum import Enum

class PodLabel(Enum):
    SERVER = 'app.kubernetes.io/component=database'
    PALLAS = ''
    META_SERVER = ''
    META_PALLAS = ''
    NFS = 'app.kubernetes.io/component=nfs'
    WORKER = 'spark-role=driver'
    EXECUTOR = 'spark-role=executor'


    def get_labels_via_name(label):
        label_list = {}

        for label_info in PodLabel:
            if label_info == label:
                labels = label_info.value.split(',')
                for label in labels:
                    l_info = str(label).split('=')

                    label_list[l_info[0]] = l_info[1]
                break

        return label_list


if __name__=="__main__":
    a = PodLabel.EXECUTOR.name
    b = PodLabel.EXECUTOR.value
    pass
