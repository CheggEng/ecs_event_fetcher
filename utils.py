# Modified to support retries based on exception type by William Jimenez <wjimenez@chegg.com>
# Originally from https://wiki.python.org/moin/PythonDecoratorLibrary#Retry

import time
import math
import logging

logger = logging.getLogger('ecs_event_fetcher')

retries = {}

class MaxConnectionRetryException(Exception):
    """
    Too many connection retry events
    """


# Retry decorator with exponential backoff
def retry(tries=5, delay=3, backoff=2, exception_type=Exception):
    '''Retries a function or method until it returns True.

  delay sets the initial delay in seconds, and backoff sets the factor by which
  the delay should lengthen after each failure. backoff must be greater than 1,
  or else it isn't really a backoff. tries must be at least 0, and delay
  greater than 0.'''
    if backoff <= 1:
        raise ValueError("backoff must be greater than 1")

    tries = math.floor(tries)
    if tries < 0:
        raise ValueError("tries must be 0 or greater")

    if delay <= 0:
        raise ValueError("delay must be greater than 0")

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            try:  # attempt to get retry state from dictionary
                mtries = retries[f.__name__]['mtries']
                mdelay = retries[f.__name__]['mdelay']
            except KeyError:
                mtries = tries
                mdelay = delay
            try:
                rv = f(*args, **kwargs)  # first attempt
                return rv  # return the output of the function to the caller
            except exception_type as e:
                while mtries > 0:
                    mtries -= 1  # consume an attempt
                    logger.error("Exception {} encountered. Retrying.".format(exception_type.__name__))
                    time.sleep(mdelay)  # wait...
                    mdelay *= backoff  # make future wait longer
                    # store value of retry state
                    retries[f.__name__]['mtries'] = mtries
                    retries[f.__name__]['mdelay'] = mdelay
                    f_retry(*args, **kwargs)  # Try again
                del retries[f.__name__]  # clear state
                raise MaxConnectionRetryException  # Ran out of tries :-(. Raise exception up the stack

        return f_retry  # true decorator -> decorated function

    return deco_retry  # @retry(arg[, ...]) -> true decorator
