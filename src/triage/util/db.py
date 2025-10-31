# coding: utf-8

import sqlalchemy
import wrapt
from contextlib import contextmanager
from sqlalchemy.orm import Session
from sqlalchemy.engine.url import make_url

import json
import functools

from psycopg2.extras import DateRange, DateTimeRange
from datetime import date, datetime


def serialize_to_database(obj, url=None):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, date):
        return str(obj.isoformat())

    if url and url.drivername in ('postgresql', 'postgresql+psycopg2', 'postgresql+psycopg2cffi'):
        if isinstance(obj, (DateRange, DateTimeRange)):
            return f"[{obj.lower}, {obj.upper}]"

    return obj


def json_dumps(d, url=None):
    return json.dumps(d, default=lambda obj: serialize_to_database(obj, url))



class SerializableDbEngine(wrapt.ObjectProxy):
    """A sqlalchemy engine that can be serialized across process boundaries.

    Works by saving all kwargs used to create the engine and reconstructs them later.  As a result, the state won't be saved upon serialization/deserialization.
    """

    __slots__ = ("url", "creator", "kwargs")

    def __init__(self, url, *, creator=sqlalchemy.create_engine, **kwargs):
        self.url = make_url(url)
        self.creator = creator
        self.kwargs = kwargs

        # Add json_serializer for PostgreSQL only
        if self.url.drivername in ('postgresql', 'postgresql+psycopg2', 'postgresql+psycopg2cffi'):
            kwargs['json_serializer'] = lambda d: json_dumps(d, self.url)

        engine = creator(url, **kwargs)
        super().__init__(engine)

    def __reduce__(self):
        return (self.__reconstruct__, (self.url, self.creator, self.kwargs))

    def __reduce_ex__(self, protocol):
        # wrapt requires reduce_ex to be implemented
        return self.__reduce__()

    @classmethod
    def __reconstruct__(cls, url, creator, kwargs):
        return cls(url, creator=creator, **kwargs)


create_engine = SerializableDbEngine

@contextmanager
def scoped_session(db_engine):
    """Provide a transactional scope around a series of operations."""
    session = Session(bind=db_engine)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_for_update(db_engine, orm_class, primary_key):
    """ Gets object from the database to updated it """
    with scoped_session(db_engine) as session:
        obj = session.query(orm_class).get(primary_key)
        yield obj
        session.merge(obj)
