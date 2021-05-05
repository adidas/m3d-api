#!/bin/sh

function array_contains() {
  local LOCAL_NEEDLE=$1
  shift

  if [[ -n "$LOCAL_NEEDLE" ]]; then
    local LOCAL_VALUE
    for LOCAL_VALUE in "$@"; do
      if [[ "$LOCAL_VALUE" == "$LOCAL_NEEDLE" ]]; then
        echo "$LOCAL_VALUE"
        break
      fi
    done
  fi
}

function validate_possible_values() {
  local LOCAL_HELP_STRING=$1
  shift

  if [[ -z $(array_contains "$@") ]]; then
    exit_with_messages "ERROR: wrong command" "$LOCAL_HELP_STRING"
  fi
}

function validate_args_non_empty() {
  local LOCAL_HELP_STRING=$1
  local LOCAL_SECOND_ARG=$2

  if [[ -z "$LOCAL_SECOND_ARG" ]]; then
    exit_with_messages "ERROR: too few arguments" "$LOCAL_HELP_STRING"
  fi
}

function validate_args_are_empty() {
  local LOCAL_HELP_STRING=$1
  local LOCAL_SECOND_ARG=$2

  if [[ -n "$LOCAL_SECOND_ARG" ]]; then
    exit_with_messages "ERROR: too many arguments" "$LOCAL_HELP_STRING"
  fi
}

function exit_with_messages() {
  local LOCAL_MESSAGE
  for LOCAL_MESSAGE in "$@"; do
    echo "$LOCAL_MESSAGE"
  done

  exit 1
}

function print_help_lines() {
  local LOCAL_TITLE=$1
  echo "$LOCAL_TITLE"
  shift

  local LOCAL_ARG_NAME
  local LOCAL_KEYS
  local LOCAL_DESCRIPTION
  for LOCAL_ARG_NAME in "$@"; do
    local LOCAL_ARRAY_NAME="${LOCAL_ARG_NAME}[@]"
    local LOCAL_ARRAY_VALUE=(${!LOCAL_ARRAY_NAME})
    if [[ "$LOCAL_ARG_NAME" == ARG_* ]]; then
        LOCAL_KEYS="${LOCAL_ARRAY_VALUE[0]}"
        LOCAL_DESCRIPTION=$(echo "${LOCAL_ARRAY_VALUE[@]:1}")
    else
        LOCAL_KEYS="-${LOCAL_ARRAY_VALUE[1]}|--${LOCAL_ARRAY_VALUE[0]}"
        LOCAL_DESCRIPTION=$(echo "${LOCAL_ARRAY_VALUE[@]:2}")
    fi
    printf "  %-20s %s\n" "$LOCAL_KEYS" "$LOCAL_DESCRIPTION"
  done
}

function exec_command_within_container() {
  local LOCAL_CONTAINER_INSTANCE_NAME=$1
  local LOCAL_PROJECT_NAME=$2
  local LOCAL_CMD=$3
  local LOCAL_IS_INTERACTIVE=$4

  if [[ -z "$LOCAL_IS_INTERACTIVE" ]]; then
    echo "Executing command within container: $LOCAL_CMD"
    docker exec "$LOCAL_CONTAINER_INSTANCE_NAME" bash -c "cd /m3d/workspace/${LOCAL_PROJECT_NAME} && ${LOCAL_CMD}"
  else
    echo "Executing command within container in interactive mode: $LOCAL_CMD"
    docker exec -it "$LOCAL_CONTAINER_INSTANCE_NAME" bash -c "cd /m3d/workspace/${LOCAL_PROJECT_NAME} && ${LOCAL_CMD}"
  fi
}

function create_container_instance_name() {
  local LOCAL_PREFIX=$1
  local LOCAL_WORKSPACE=$2

  echo "${LOCAL_PREFIX}-${LOCAL_WORKSPACE//[^a-zA-Z0-9-]/}"
}
