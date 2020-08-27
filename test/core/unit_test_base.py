import os
import shutil
import logging

import py


class UnitTestBase(object):

    local_global_test_home = os.path.abspath("test/runs/")
    local_suite_home = None
    local_case_home = None

    local_run_dir = None  # py.path.local object pointing to local_case_home

    patches = [
        # needed mocks can be placed here
        # mock.patch('my_mock', new=myConnectionMock, create=True)
    ]

    @classmethod
    def setup_class(cls):
        cls.local_suite_home = os.path.join(cls.local_global_test_home, cls.__name__)

        for p in cls.patches:
            p.start()

        logging.info("Performing cleanup of local directory: \"%s\"" % cls.local_suite_home)
        if not os.path.exists(cls.local_global_test_home):
            os.mkdir(cls.local_global_test_home)

        if os.path.exists(cls.local_suite_home):
            shutil.rmtree(cls.local_suite_home)

        os.mkdir(cls.local_suite_home)

    def setup_method(self, method):
        self.local_case_home = os.path.join(self.local_suite_home, method.__name__)

        if os.path.exists(self.local_case_home):
            shutil.rmtree(self.local_case_home)

        os.mkdir(self.local_case_home)

        self.local_run_dir = py.path.local(self.local_case_home)

    @classmethod
    def teardown_class(cls):
        logging.info("Performing teardown_class() for  \"%s\" class." % cls.__name__)
        for p in cls.patches:
            p.stop()

    @staticmethod
    def setup_file_parent_directory(file_path):
        if not os.path.isdir(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))
