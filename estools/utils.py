from __future__ import print_function
import time

from datetime import timedelta


def wait_for(condition, timeout=timedelta(seconds=30), retry_period=timedelta(seconds=10), ignored_exceptions=[]):
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


class TimeoutException(BaseException):
    pass
