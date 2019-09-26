import functools
import logging


def exception_logger(exception_class):
    """
    A decorator that wraps the passed in function and logs exceptions of given type should one occur.
    It will also re-raise exception.

    @param exception_class: class of exceptions that should be logged
    """

    def decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception_class as e:
                logging.error(str(e))
                raise

        return wrapper

    return decorator
