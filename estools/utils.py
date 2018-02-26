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
    """
    >>> from freezegun import freeze_time
    >>> from datetime import timedelta
    >>> from mock import patch
    >>> with freeze_time() as t:
    ...   timed(action=lambda: t.tick(timedelta(seconds=10)), message="my message")

    """
    start_time = time.time()
    try:
        return action()
    finally:
        end_time = time.time()
        logger = logging.getLogger('estools.timer')
        duration_in_seconds = end_time - start_time
        logger.debug('%s took %s seconds', message, duration_in_seconds)


class TimeoutException(Exception):
    pass
