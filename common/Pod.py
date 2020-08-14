

class Pod(object):
    _pod_ip = None
    _pod_status = None
    _pod_node = None
    _pod_name = None

    def __init__(self):
        pass

    @property
    def pod_name(self):
        return self._pod_name

    @pod_name.setter
    def pod_name(self, value):
        self._pod_name = value

    @property
    def pod_ip(self):
        return self._pod_ip

    @pod_ip.setter
    def pod_ip(self, value):
        self._pod_ip = value

    @property
    def pod_status(self):
        return self._pod_status

    @pod_status.setter
    def pod_status(self, value):
        self._pod_status = value

    @property
    def pod_node(self):
        return self._pod_node

    @pod_node.setter
    def pod_node(self, value):
        self._pod_node = value