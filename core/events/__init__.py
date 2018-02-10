# -*- coding: utf-8 -*-

"""

    Module :mod:``


    LICENSE: The End User license agreement is located at the entry level.

"""

# ----------- START: Native Imports ---------- #
__import__('pkg_resources').declare_namespace(__name__)
# ----------- END: Native Imports ---------- #

# ----------- START: Third Party Imports ---------- #
import sqlite3

from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
# ----------- END: Third Party Imports ---------- #

# ----------- START: In-App Imports ---------- #
# ----------- END: In-App Imports ---------- #

__all__ = [
    # All public symbols go here.
]


class DataBaseEntity(object):
    """."""
    instances = list()

    def __init__(self, query, pre_query='', desc=''):
        """."""

        self.query = query
        self.pre_query = pre_query or None
        self.description = desc

        self.__class__.instances.append(self)

    @classmethod
    def load_all(cls):
        import sqlite3 as sqlite
        connection = sqlite.connect('db.sqlite')

        cursor = connection.cursor()

        for each in cls.instances:
            if each.pre_query:
                cursor.execute(each.pre_query)
            cursor.execute(each.query)

        connection.commit()


def create_session():
    """."""
    # application starts
    Session = sessionmaker()
    conn = sqlite3.connect('example.db')

    #c = conn.cursor()
    # Create table
    #c.execute('''CREATE TABLE user (id number, username text, password text)''')
    #c.execute('''insert into user (id, username, password) values (1, 'admin', 'admin')''')
    #ss = c.execute('''select * from user''')

    # ... later
    engine = create_engine('sqlite:///db.sqlite')
    Session.configure(bind=engine)

    session = Session()

    #session.execute('''insert into user (id, user, passwd) values (1, 'siva', 'sri')''')
    return session
