# -*- coding:utf-8 -*-


class K8SCustomObject(object):
    _name = None
    _namespace = None
    _group = None
    _version = None
    _plural = None

    def __init__(self):
        pass

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def namespace(self):
        return self._namespace

    @namespace.setter
    def namespace(self, value):
        self._namespace = value

    @property
    def group(self):
        return self._group

    @group.setter
    def group(self, value):
        self._group = value

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, value):
        self._version = value

    @property
    def plural(self):
        return self._plural

    @plural.setter
    def plural(self, value):
        self._plural = value

