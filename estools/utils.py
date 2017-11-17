from __future__ import print_function

import logging
import time

from datetime import timedelta


def wait_for(condition, timeout=timedelta(seconds=30), retry_period=timedelta(seconds=10), ignored_exceptions=[]):
    """
    >>> wait_for(
    ...     condition=lambda: True,
    ...     timeout=timedelta(seconds=2),
    ...     retry_period=timedelta(seconds=1))
    >>> wait_for(
    ...     condition=lambda: False,
    ...     timeout=timedelta(seconds=2),
    ...     retry_period=timedelta(seconds=1))
    Traceback (most recent call last):
      ...
    TimeoutException
    >>> def raise_key_error():
    ...     raise KeyError()
    >>> wait_for(
    ...     condition=raise_key_error,
    ...     timeout=timedelta(seconds=2),
    ...     retry_period=timedelta(seconds=1),
    ...     ignored_exceptions=[KeyError])
    Traceback (most recent call last):
      ...
    TimeoutException
    >>> wait_for(
    ...     condition=raise_key_error,
    ...     timeout=timedelta(seconds=2),
    ...     retry_period=timedelta(seconds=1))
    Traceback (most recent call last):
      ...
    KeyError
    """
    assert isinstance(timeout, timedelta)
    assert isinstance(retry_period, timedelta)

    start_time = time.time()
    while True:
        try:
            if condition():
                return
        except Exception as e:
            search = [x for x in ignored_exceptions if isinstance(e, x)]
            if len(search) == 0:
                raise e
        print('.', end='')
        time.sleep(retry_period.total_seconds())
        if time.time() > start_time + timeout.total_seconds():
            raise TimeoutException()


def timed(action, message):
    start_time = time.time()
    try:
        action()
    finally:
        end_time = time.time()
        logger = logging.getLogger('estools.timer')
        logger.debug('%s took %s seconds', message, end_time - start_time)


class TimeoutException(Exception):
    pass
