#!/bin/bash

set -e

SCRIPT_NAME="dev-env.sh"
PROJECT_NAME="m3d-api"
CONTAINER_IMAGE_NAME="$PROJECT_NAME"

PARAM_WORKSPACE=(    "workspace"   "w" "m3d-engine code directory (must be the same within the container life-cycle)")
PARAM_TEST_TYPE=(    "test-type"   "t" "type of tests to run, possible values are [unit|integration|all]")
PARAM_TEST_MARK=(    "test-mark"   "m" "pytest mark for filtering tests, possible values are [bdp|emr|algo|oracle]")
OPTION_HELP=(        "help"        "h" "show help message for the command")
OPTION_INTERACTIVE=( "interactive" "i" "use interactive mode and allocate pseudo-TTY when executing a command inside the container")

ARG_ACTION_IMAGE_BUILD=(       "image-build"       "build the docker image")
ARG_ACTION_CONTAINER_RUN=(     "container-run"     "run a container from the docker image")
ARG_ACTION_CONTAINER_EXECUTE=( "container-execute" "execute an external command within the container")
ARG_ACTION_CONTAINER_STOP=(    "container-stop"    "stop the container")
ARG_ACTION_CONTAINER_DELETE=(  "container-delete"  "delete the container")
ARG_ACTION_PROJECT_LINT=(      "project-lint"      "lint the code within the container")
ARG_ACTION_PROJECT_COMPILE=(   "project-compile"   "compile the code within the container")
ARG_ACTION_PROJECT_TEST=(      "project-test"      "run tests within the container")
ARG_ACTION_PROJECT_CLEAN=(     "project-clean"     "clean pyc-files in the project directory")
ARG_COMMAND=(                  "command"           "command to execute within the container")

AVAILABLE_ACTIONS=(\
    "$ARG_ACTION_IMAGE_BUILD" \
    "$ARG_ACTION_CONTAINER_RUN" \
    "$ARG_ACTION_CONTAINER_EXECUTE" \
    "$ARG_ACTION_CONTAINER_STOP" \
    "$ARG_ACTION_CONTAINER_DELETE" \
    "$ARG_ACTION_PROJECT_LINT" \
    "$ARG_ACTION_PROJECT_COMPILE" \
    "$ARG_ACTION_PROJECT_TEST" \
    "$ARG_ACTION_PROJECT_CLEAN" \
)

source "./common.sh"

###   PROJECT-SPECIFIC FUNCTIONS   ###

function create_actions_help_string() {
  echo "Usage: $SCRIPT_NAME action [-h] [-w workspace]"
  print_help_lines "Supported actions:" \
    "ARG_ACTION_IMAGE_BUILD" \
    "ARG_ACTION_CONTAINER_RUN" \
    "ARG_ACTION_CONTAINER_EXECUTE" \
    "ARG_ACTION_CONTAINER_STOP" \
    "ARG_ACTION_CONTAINER_DELETE" \
    "ARG_ACTION_PROJECT_LINT" \
    "ARG_ACTION_PROJECT_COMPILE" \
    "ARG_ACTION_PROJECT_TEST" \
    "ARG_ACTION_PROJECT_CLEAN"

  print_help_lines "Parameters:" "PARAM_WORKSPACE"
  print_help_lines "Options:" "OPTION_HELP"
}

function create_action_default_help_string() {
  local LOCAL_ACTION=$1
  echo "Usage: $SCRIPT_NAME $LOCAL_ACTION [-h] [-w workspace]"
  print_help_lines "Parameters:" "PARAM_WORKSPACE"
  print_help_lines "Options:" "OPTION_HELP"
}

function create_action_execute_help_string() {
  echo "Usage: $SCRIPT_NAME $ARG_ACTION_CONTAINER_EXECUTE [-h] [-i] [-w workspace] command"
  print_help_lines "Arguments:" "ARG_COMMAND"
  print_help_lines "Parameters:" "PARAM_WORKSPACE"
  print_help_lines "Options:" "OPTION_INTERACTIVE" "OPTION_HELP"
}

function create_action_run_tests_help_string() {
  echo "Usage: $SCRIPT_NAME $ARG_ACTION_PROJECT_TEST [-h] [-i] [-w workspace] [-m test_mark] [-t test_type]"
  print_help_lines "Parameters:" "PARAM_WORKSPACE" "PARAM_TEST_TYPE" "PARAM_TEST_MARK"
  print_help_lines "Options:" "OPTION_INTERACTIVE" "OPTION_HELP"
}

function create_action_build_help_string() {
  echo "Usage: $SCRIPT_NAME build [-h]"
  print_help_lines "Options:" "OPTION_HELP"
}

###   ARGUMENT PARSING   ###

ACTION=$(array_contains "$1" "${AVAILABLE_ACTIONS[@]}")
if [[ -z "$ACTION" ]]; then
  HELP_STRING=$(create_actions_help_string)
  exit_with_messages "ERROR: wrong action specified" "$HELP_STRING"
else
  shift
  if [[ "$ACTION" == "$ARG_ACTION_IMAGE_BUILD" ]]; then
    HELP_STRING=$(create_action_build_help_string)
    ACTION_AVAILABLE_ARGS=("$OPTION_HELP")
  elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_EXECUTE" ]]; then
    HELP_STRING=$(create_action_execute_help_string)
    ACTION_AVAILABLE_ARGS=("$PARAM_WORKSPACE" "$OPTION_INTERACTIVE")
  elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_TEST" ]]; then
    HELP_STRING=$(create_action_run_tests_help_string)
    ACTION_AVAILABLE_ARGS=("$PARAM_WORKSPACE" "$PARAM_TEST_TYPE" "$PARAM_TEST_MARK" "$OPTION_INTERACTIVE")
  else
    HELP_STRING=$(create_action_default_help_string "$ACTION")
    ACTION_AVAILABLE_ARGS=("$PARAM_WORKSPACE")
  fi
fi

OTHER_ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -w|--workspace)
      shift
      validate_args_non_empty "$HELP_STRING" "$@"
      WORKSPACE="$1";;
    -t|--test-type)
      shift
      validate_args_non_empty "$HELP_STRING" "$@"
      validate_possible_values "$HELP_STRING" "$PARAM_TEST_TYPE" "${ACTION_AVAILABLE_ARGS[@]}"
      TEST_TYPE="$1";;
    -m|--test-mark)
      shift
      validate_args_non_empty "$HELP_STRING" "$@"
      validate_possible_values "$HELP_STRING" "$PARAM_TEST_MARK" "${ACTION_AVAILABLE_ARGS[@]}"
      TEST_MARK="$1";;
    -i|--interactive)
      validate_possible_values "$HELP_STRING" "$OPTION_INTERACTIVE" "${ACTION_AVAILABLE_ARGS[@]}"
      INTERACTIVE=1;;
    -h|--help)
      exit_with_messages "$HELP_STRING";;
    *)
      OTHER_ARGS+=("$1")
  esac
  shift
done

if [[ -z "$WORKSPACE" ]]; then
  WORKSPACE=$(pwd)
fi

CONTAINER_INSTANCE_NAME=$(create_container_instance_name "$CONTAINER_IMAGE_NAME" "$WORKSPACE")

###   ACTION HANDLERS   ###

# build the docker image
if [[ "$ACTION" == "$ARG_ACTION_IMAGE_BUILD" ]]; then
  echo "Creating a docker image $CONTAINER_IMAGE_NAME ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  docker build -t "$CONTAINER_IMAGE_NAME" .

# run a container from the docker image
elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_RUN" ]]; then
  echo "Running the container $CONTAINER_INSTANCE_NAME ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  docker run -t -d --name "$CONTAINER_INSTANCE_NAME" -v "${WORKSPACE}:/root/workspace/${PROJECT_NAME}" "$CONTAINER_IMAGE_NAME"

# clean pyc-files in the project directory
elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_CLEAN" ]]; then
  echo "Cleaning workspace directory ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  echo "Cleaning old pyc-files ..."
  COMPILE_CLEANUP_CMD="find . -name \"*.pyc\" -delete && find . -name \"__pycache__\" -delete"
  exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$COMPILE_CLEANUP_CMD"

  echo "Cleaning tests output ..."
  CACHE_CLEANUP_CMD="rm -rf ./.pytest_cache ./test/runs ./.coverage ./m3d-engine-assembly-1.0.jar"
  exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$CACHE_CLEANUP_CMD"

# lint the code in the container
elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_LINT" ]]; then
  echo "Linting the code ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  LINT_CMD="python3 -m flake8 --config ./flake8.conf"
  exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$LINT_CMD"

# build the code in the container
elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_COMPILE" ]]; then
  echo "Compiling the code ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  COMPILE_CMD="python3 -m compileall -f ./"
  exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$COMPILE_CMD"

# execute a command in the container
elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_EXECUTE" ]]; then
  echo "Executing command ..."
  if [[ "${#OTHER_ARGS[@]}" != 1 ]]; then
    exit_with_messages "ERROR: too many arguments" "$HELP_STRING"
  else
    EXTERNAL_CMD="${OTHER_ARGS[0]}"
    exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$EXTERNAL_CMD" "$INTERACTIVE"
  fi

# execute tests in the container
elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_TEST" ]]; then
  echo "Executing tests ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  AVAILABLE_TEST_TYPES=("all" "unit" "integration")
  AVAILABLE_TEST_MARKS=("bdp" "emr" "algo" "oracle")

  if [[ -z "$TEST_TYPE" ]]; then
    RUN_TESTS_CMD="python3 ./test/test_runner.py all"
  elif [[ -z $(array_contains "$TEST_TYPE" "${AVAILABLE_TEST_TYPES[@]}") ]]; then
    exit_with_messages "ERROR: wrong test type" "$HELP_STRING"
  else
    RUN_TESTS_CMD="python3 ./test/test_runner.py $TEST_TYPE"
  fi

  if [[ -z "$TEST_MARK" ]]; then
    exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$RUN_TESTS_CMD" "$INTERACTIVE"
  elif [[ -z $(array_contains "$TEST_MARK" "${AVAILABLE_TEST_MARKS[@]}") ]]; then
    exit_with_messages "ERROR: wrong test mark" "$HELP_STRING"
  else
    exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$RUN_TESTS_CMD --mark $TEST_MARK" "$INTERACTIVE"
  fi

# stop the container
elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_STOP" ]]; then
  echo "Terminating dev environment ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  echo "Stopping the container ..."
  (docker stop "$CONTAINER_INSTANCE_NAME" 1>/dev/null && echo "The container was stopped")

# delete the container
elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_DELETE" ]]; then
  echo "Terminating dev environment ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  echo "Deleting the container ..."
  (docker rm "$CONTAINER_INSTANCE_NAME" 1>/dev/null && echo "The container was deleted")

# wrong action is specified
else
  exit_with_messages "ERROR: handler for the \"${ACTION}\" action is not defined"
fi
