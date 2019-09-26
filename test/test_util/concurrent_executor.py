import logging
import time
import threading
import sys


class ConcurrentExecutor(object):

    def __init__(self, method, delay_sec=None):
        logging.info("delay_sec={}".format(delay_sec))
        logging.info("method={}".format(method.__name__))
        self.method = method
        self.child_thread_exc = None

        logging.info("Creating concurrent thread for method \"{}\"".format(method.__name__))

        def start_method():
            try:
                if delay_sec:
                    logging.info("Sleep for {} seconds before starting thread.".format(delay_sec))
                    time.sleep(delay_sec)

                method()

            except Exception as exc:
                # We will catch the exception and re-raise it in main thread.
                logging.error(str(exc))
                self.child_thread_exc = sys.exc_info()

        self.thread = threading.Thread(target=start_method)

    def __enter__(self):
        try:
            logging.info("Starting concurrent thread for method \"{}\"".format(self.method.__name__))
            self.thread.start()
        except Exception:
            # If __enter__() raises then __exit__() will not be called automatically.
            # We have to call it manually to make sure thread is terminated.
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        logging.info("Stopping concurrent thread for method \"{}\"".format(self.method.__name__))
        self.thread.join()

        if self.child_thread_exc:
            logging.warning("Re-raising exception from child thread.")
            raise self.child_thread_exc[0](self.child_thread_exc[1]).with_traceback(self.child_thread_exc[2])
