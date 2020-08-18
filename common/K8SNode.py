# -*- coding:utf-8 -*-


class K8SNode(object):
    _node_name = None
    _node_labels = None

    def __init__(self):
        pass

    @property
    def node_name(self):
        return self._node_name

    @node_name.setter
    def node_name(self, value):
        self._node_name = value

    @property
    def node_labels(self):
        return self._node_labels

    @node_labels.setter
    def node_labels(self, value):
        self._node_labels = value
