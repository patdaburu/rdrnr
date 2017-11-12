#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: tasks
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Provide a brief description of the module.
"""

from luigi.contrib.postgres import PostgresTarget
from .environment import PgEnvironment
from ...tasks import Task


def pg_info(env: str, table: str, update_id: str):
    """
    Use this decorator to indicate the PostGIS Environment that will be used by this task.
    :param env: the PostGIS environment key
    :param update_id: and identifier for the data set
    """
    def set_pg_info(cls):
        # Set the flag that tells us we actually have some info supplied by the decorator.
        cls._pg_info = True
        # Set the PostGIS environment key.
        cls._pgenv_key = env
        # Set the update ID.
        cls._target_update_id = update_id
        # Set the target table.
        cls._target_table
        return cls
    return set_pg_info()


class PgTaskMixin(object):
    _pg_info: bool = False  #: Does the task have PostGIS information applied to it?
    _pgenv_key: str = 'default'  #: the environment key
    _target_table: str = None
    _target_update_id: str = None  #: the update ID for the Postgres target

    def __init__(self):
        # If no PostGIS information has been supplied...
        if not self._pg_info:
            raise TypeError('No PostGIS information has been supplied for this task.')
        # If PostGIS information has been supplied...
        if self._pg_info:
            # ...let's get set up.
            self._pgenv = PgEnvironment.instance(self._pgenv_key)
            self._pg_target = PostgresTarget(host=self._pgenv.host,
                                             database=self._pgenv.database,
                                             user=self._pgenv.user,
                                             password=self._pgenv.password,
                                             table=self._target_table,
                                             update_id=self._target_update_id)


class PgTask(Task, PgTaskMixin):
    def __init__(self):
        Task.__init__(self)
        PgTaskMixin.__init__(self)



