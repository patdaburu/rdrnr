#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: tasks
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Tasks are those things that we do when we want to make things happen.
"""

import luigi
from abc import ABCMeta, abstractmethod
from enum import Enum, IntEnum
from hoonds.errors import CustomException
from hoonds.patterns.observer import Observable, signals, SignalArgs
from typing import Dict, List


def requires(prqs: luigi.Task or List[luigi.Task] or type):
    """
    Use this decorator to define a tasks prerequisites.
    :param prqs: the prerequisite tasks
    """
    def add_require(cls):
        # If the class doesn't already have the _prqs list...
        if not hasattr(cls, '_prqs'):
            cls._prqs = []  # ...it does now!
        # If we got a list of requirements, we can use them as they are, but if we got a single requirement we need
        # to put it into a list.
        _prqs = prqs if isinstance(prqs, list) else [prqs]
        # Now we need to make sure everything in the list is actually an instance.
        _prqs = [r() if isinstance(r, type) else r for r in _prqs]
        # Now we can add all these requirements.
        # noinspection PyProtectedMember
        cls._prqs.extend(_prqs)
        return cls
    return add_require


class TaskMixin(object):
    """
    This mixin provides common functionality for tasks.
    """
    def __init__(self):
        super().__init__()
        # If prerequisites were defined by a decorator...
        if hasattr(self.__class__, '_prqs'):
            # ...replace the class' requires() method.
            self.requires = self._get_prqs
        # Keep a reference to the original 'run' method.
        self._run_method = self.run
        # The 'run' method (as far as outsiders are concerned) is now the 'supervised run' method.
        self.run = self._run_supervised

    def _get_prqs(self):
        """
        Get the list of prerequisite tasks.
        :return: the list of prerequisite tasks
        """
        return self._prqs

    def _run_supervised(self):
        """
        This method supervises and calls the original run() method defined for the class.
        :return:
        """
        print('READY')
        self._run_method()
        print('DONE')


class Task(luigi.Task, TaskMixin):
    """
    Extend this class to define a chunk of logic that should be executed as a standard task.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        luigi.Task.__init__(self)
        TaskMixin.__init__(self)


