import os
import argparse
import pytest
import inspect
import sys


# test type to pytest args map
test_args = {
    "unit": {
        "cmd": "--cov=m3d --log-cli-level=INFO --color=yes -x -v ./test/unit",
        "mark": None
    },
    "integration": {
        "cmd": "--cov=m3d --log-cli-level=INFO --color=yes -x -v ./test/integration",
        "mark": None
    },
    "all": {
        "cmd": "--cov=m3d --log-cli-level=INFO --color=yes -x -v ./test",
        "mark": None
    }
}


if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description='Wrapper for schema creation')
    parser.add_argument('test_type', choices=test_args.keys(), help="test type to run")
    parser.add_argument('-m', "--mark", action="store", dest="mark", default=None, help="mark to use")

    args = parser.parse_args()

    pytest_args = test_args[args.test_type]["cmd"].split()
    pytest_marks = []

    if test_args[args.test_type]["mark"]:
        pytest_marks.append(test_args[args.test_type]["mark"])

    if args.mark:
        pytest_marks.append(args.mark)

    if pytest_marks:
        pytest_marks_str = " and ".join(pytest_marks)
        pytest_args.extend(["-m", pytest_marks_str])

    # We need to call pytest from m3d-api root directory.
    test_runner_file_path = os.path.abspath(inspect.getfile(inspect.currentframe()))
    m3d_api_root = os.path.dirname(os.path.dirname(test_runner_file_path))

    # To fix package discovery we need to adjust sys.path to point to m3d-api root
    sys.path[0] = m3d_api_root

    os.chdir(m3d_api_root)

    ret_code = pytest.main(pytest_args)
    exit(ret_code)
