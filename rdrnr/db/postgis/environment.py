#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: environment
.. moduleauthor:: Pat Daburu <pat@daburu.net>

Provide a brief description of the module.
"""

from CaseInsensitiveDict import CaseInsensitiveDict
from typing import Dict

class PgEnvironment(object):

    _environments: Dict[str, 'PgEnvironment'] = CaseInsensitiveDict()

    def __init__(self,
                 host: str='localhost',
                 port: int=5432,
                 database: str=None,
                 user: str=None,
                 password: str=None):
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password

    @property
    def host(self) -> str:
        return self._user

    @property
    def port(self) -> int:
        return self._port

    @property
    def database(self) -> str:
        return self._database

    @property
    def user(self) -> str:
        return self._user

    @property
    def password(self) -> str:
        return self._password

    @staticmethod
    def initialize(
            key: str='default',
            host: str='localhost',
            port: int=5432,
            database: str=None,
            user: str=None,
            password: str=None) -> 'PgEnvironment':
        if key in PgEnvironment._environments:
            raise KeyError("The {key} environment has already been initialized.".format(key=key))
        else:
            # Initialize the environment.
            env = PgEnvironment(host=host, port=port, database=database,
                                user=user, password=password)
            # Update the dictionary.
            PgEnvironment._environments[env] = env
            # Return the new environment to the caller.
            return env

    @staticmethod
    def instance(key: str='default') -> 'PgEnvironment':
        """
        Get the current environment instance.
        :return: the current environment instance
        """
        return PgEnvironment._environments[key]
