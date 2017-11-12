#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: tasks
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Provide a brief description of the module.
"""

from luigi.contrib.postgres import PostgresTarget
from ...tasks import Task

def pg_environment(key: str):
    """
    Use this decorator to define a tasks prerequisites.
    :param prqs: the prerequisite tasks
    """
    def set_pgenv(cls):
        cls._pgenv = key
        return cls
    return set_pgenv()

class PgTask(Task):

    _pgenv: str = 'default'
