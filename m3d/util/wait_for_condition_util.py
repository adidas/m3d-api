import datetime
import logging
import random
import sys
import time


class WaitForConditionUtil(object):

    @staticmethod
    def _sleep_seconds_generator(polling_interval_seconds):
        """
        Function yields seconds in range [polling_interval_seconds / 2, polling_interval_seconds] and
        the very first value will be 0.
        """
        yield 0

        # making sure it is float
        polling_interval_seconds = polling_interval_seconds * 1.0

        while True:
            yield random.uniform(polling_interval_seconds / 2, polling_interval_seconds)

    @staticmethod
    def wait_for_condition(condition_predicate, timeout_seconds, period_seconds, timeout_err):
        end_datetime = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout_seconds)
        sleep_seconds_generator = WaitForConditionUtil._sleep_seconds_generator(period_seconds)

        cur_exc_info = None

        while datetime.datetime.utcnow() <= end_datetime:
            time.sleep(next(sleep_seconds_generator))
            cur_exc_info = None

            try:
                condition_flag, output = condition_predicate()

                if condition_flag:
                    return output

            except Exception as exc:
                logging.error(str(exc))
                cur_exc_info = sys.exc_info()

        # If there is an active exception, then we will report that instead of timeout.
        if cur_exc_info:
            raise cur_exc_info[0](cur_exc_info[1]).with_traceback(cur_exc_info[2])

        logging.error(str(timeout_err))
        raise timeout_err
