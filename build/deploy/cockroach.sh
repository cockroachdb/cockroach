#!/bin/bash
#
# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.
set -eu

COCKROACH_ENTRYPOINT="/cockroach/cockroach"

INSECURE=
CERTS_DIR=
URL=
# If user doesn't specify the address to listen to, use the default listen
# address.
LISTEN_ADDR_PORT="127.0.0.1:26257"
LISTEN_ADDR="127.0.0.1"

# parse_command_line is to extract the values assigned to certain flags
# in the command.
parse_command_line() {
  local prev=
  local optarg=
  for argument; do
     # If the previous option needs an argument, assign it.
     if test -n "$prev"; then
        eval $prev=\$argument
        prev=
        continue
     fi

      case "$argument" in
          *=?*) optarg=`expr "X$argument" : '[^=]*=\(.*\)'` ;;
          *=)   optarg= ;;
          *)    optarg=true ;;
      esac

      case "$argument" in
          --insecure)        INSECURE=true ;;
          --certs-dir)       prev=CERTS_DIR ;;
          --certs-dir=*)     CERTS_DIR=$optarg ;;
          --url)             prev=URL ;;
          --url=*)           URL=$optarg ;;
          --listen-addr)     prev=LISTEN_ADDR_PORT ;;
          --listen-addr=*)   LISTEN_ADDR_PORT=$optarg ;;
      esac
  done

  if [[ -n "$INSECURE" ]] && [[ -n "$CERTS_DIR" ]]; then
      echo >&2 "error: cannot use certs in insecure mode"
      exit 1
  fi

  if [[ -z "$INSECURE" ]] && [[ -z "$CERTS_DIR" ]]; then
    echo >&2 "using insecure mode by default"
    INSECURE=true
  fi

  # If listen address is explicitly assigned with `--listen-addr`
  # (e.g. --listen-addr=127.0.0.2:12345), we replace the LISTEN_ADDR with the
  # assigned one.
  # Otherwise, the listening address is not explicitly assigned (e.g.
  # --listen-addr=:12345), we use the default 127.0.0.1 for LISTEN_ADDR.
  if [[ -n ${LISTEN_ADDR_PORT%%:*} ]]; then
    LISTEN_ADDR=${LISTEN_ADDR_PORT%%:*}
  fi
}

# setup_certs_dir is to set up the certs dir.
setup_certs_dir() {
  mkdir -p "$CERTS_DIR"

  if ! [[ $(ls -A "$CERTS_DIR") ]]; then
    $COCKROACH_ENTRYPOINT cert create-ca --certs-dir="$CERTS_DIR" \
    --ca-key="$CERTS_DIR"/ca.key
    $COCKROACH_ENTRYPOINT cert create-client --certs-dir="$CERTS_DIR" \
    --ca-key="$CERTS_DIR"/ca.key root
  fi

  if ! ls "$CERTS_DIR"/node.* &>/dev/null; then
    $COCKROACH_ENTRYPOINT cert create-node --certs-dir="$CERTS_DIR" \
    --ca-key="$CERTS_DIR"/ca.key "$LISTEN_ADDR"
  fi

  echo "certificate dir \"$CERTS_DIR\" is successfully set up"
}

# set_env_var is to set the an environment variable with given value.
# The value is optional, and by default it's an empty string.
# usage: set_env_var VAR_NAME VALUE
set_env_var() {
  local env_var="$1"
  local val="${2:-}"
  if [ "${!env_var:-}" ]; then
    val="${!env_var}"
  fi
  export "$env_var"="$val"
}

# setup_env is to set up the environment variables.
setup_env() {
  set_env_var "COCKROACH_DATABASE" "defaultdb"
  set_env_var "COCKROACH_USER"
  set_env_var "COCKROACH_PASSWORD"
}

# start_init_node is to start the single node for the initialization.
start_init_node() {
  echo "starting node for the initialization process. This could take a couple seconds..."
  rm -f server_fifo; mkfifo server_fifo
  local start_node_query=( "$COCKROACH_ENTRYPOINT" start-single-node \
                           --background --listening-url-file=server_fifo \
                           --pid-file=server_pid --listen-addr="$LISTEN_ADDR_PORT" \
                           --certs-dir="$CERTS_DIR" )

  # Check if the given command contains the `--insecure` flag.
  if [[ -n "$INSECURE" ]]; then
    start_node_query+=( --insecure )
  fi

  # Start the node and run in the background.
  "${start_node_query[@]}" &

  # Set a 5-minute timeout for the single node starting up.
  timeout 3000 cat server_fifo>server.url
  exit_status=$?
  if [[ $exit_status -eq 124 ]]; then
    echo >&2 "error: timeout for cockroach init process"
    exit 1
  fi
  echo >&2 "init node successfully started"
}

# setup_db is to create a default database, create a default user (with password if applicable),
# and grant privileges to this user by running `cockroach sql` queries.
setup_db() {
  create_defaultdb
  create_default_user

  local init_env_query=( --certs-dir="$CERTS_DIR" )
  if [[ -n "$COCKROACH_USER" ]]; then
      init_env_query+=( -e "GRANT ALL ON DATABASE "$COCKROACH_DATABASE" TO "$COCKROACH_USER"" \
                        -e "GRANT admin TO "$COCKROACH_USER" " )
  fi

  # Check if the given command contains the `--insecure` flag.
  # If not, add `--certs-dir=certs` to init_env_query.
  if [[ -n "$INSECURE" ]]; then
    init_env_query+=( --insecure )
  fi

  run_sql_query "${init_env_query[@]}"
}

# process_init_files run all the init scripts from /docker-entrypoint-initdb.d.
# This is largely based on PostgreSQL's docker-entrypoint.sh:
# https://github.com/docker-library/postgres/blob/48a0a3600d170eeafa09372ab5af95b7fdc89c23/14/alpine/docker-entrypoint.sh#L153-L182
# usage: process_init_files [file [file [...]]]
# e.g. process_init_files /your_folder/*
process_init_files() {
  for f in "$@"; do
    case "$f" in
      *.sh)
        if [ -x "$f" ]; then
          echo "$0: running $f"
          "$f"
        else
          echo "$0: sourcing $f"
          . "$f"
        fi
        ;;
      *.sql)    echo "$0: running $f"; run_sql_query -f "$f"; echo ;;
      *.sql.gz) echo "$0: running $f"; gzip -cd "$f" | run_sql_query; echo ;;
      *.sql.xz) echo "$0: running $f"; xz --decompress --stdout "$f" | \
                run_sql_query; echo ;;
      *)        echo "$0: ignoring $f" ;;
    esac
    echo
  done
}

# stop_init_node is to stop the single node for the initialization.
stop_init_node() {
  kill $(cat server_pid)
  local TIMEOUT=100
  local TIME_COUNTER=0
  until [[ "$TIME_COUNTER" -eq "$TIMEOUT" ]] || kill -0 $(cat server_pid); do
    echo >&2 "finishing cockroach init process"
    sleep 2
    TIME_COUNTER=$((TIME_COUNTER+1))
  done
  check_if_server_fully_stopped
  echo >&2 "cockroach init process finished, restart the server now"
}

# run_sql_query is a helper function to run sql queries.
run_sql_query() {
  $COCKROACH_ENTRYPOINT sql --url="$(cat server.url)" "$@"
}

# db_already_exists runs a sql query to check if the database already exists.
db_already_exists() {
   run_sql_query -e "select database_name FROM [SHOW DATABASES]" \
     | tail -n +2 \
     | grep "^${1}\$"
}

# user_already_exists runs a sql query to check if the user already exists.
user_already_exists() {
   run_sql_query -e "select username FROM [SHOW ROLES]" \
     | tail -n +2 \
     | grep "^${1}\$"
}

# create_defaultdb is to create a default database. If there doesn't exist a
# database with the given name, create a new database. Otherwise, no-op.
create_defaultdb() {
  # Check if this database name specified in COCKROACH_DATABASE already exists.
  # If not, create a new database.
  if [[ -z "$(db_already_exists "$COCKROACH_DATABASE")" ]]; then
    run_sql_query -e "CREATE DATABASE $COCKROACH_DATABASE"
    echo >&2 "finished creating default database \"$COCKROACH_DATABASE\""
  else
    echo >&2 "database \"$COCKROACH_DATABASE\" already exists"
  fi
}

# create_default_user is to create a default user. If there doesn't exist a user
# with the given name, create a new user. Otherwise, no-op.
create_default_user() {
  # If the `COCKROACH_USER` env var is unset, do not proceed to create a new user.
  if [[ -z $COCKROACH_USER ]]; then
    return 0
  fi
  # Check if the username specified in COCKROACH_USER already exists.
  # If not, create a new user.
  if [[ -z "$(user_already_exists "$COCKROACH_USER")" ]]; then
    # Create a new user with the given name.
    local create_user_query="CREATE USER "$COCKROACH_USER""
    if [[ -n "$COCKROACH_PASSWORD" ]]; then
          create_user_query+=" WITH PASSWORD '$COCKROACH_PASSWORD'"
    fi
    run_sql_query -e "$create_user_query"
    echo >&2 "finished creating default user \"$COCKROACH_USER\""
  else
    echo >&2 "user \"$COCKROACH_USER\" already exists"
  fi
}

# check_if_server_fully_stopped is to wait until the init server is fully stopped or timeout.
check_if_server_fully_stopped() {
  local LOG_PATH=./cockroach-data/logs/cockroach.log
  local TIMEOUT=20
  local TIME_COUNTER=0

  until [[ "$TIME_COUNTER" -eq "$TIMEOUT" ]] || [[ -f "$LOG_PATH" ]]; do
    echo >&2 "$LOG_PATH doesn't exist, waiting ..."
    sleep 2
    ((TIME_COUNTER+=1))
  done

  # If timeout, exit the program.
  if [[ "$TIME_COUNTER" -ge "$TIMEOUT" ]]; then
    echo >&2 "error: timeout for finding log file"
    exit 1
  fi

  TIME_COUNTER=0
  echo >&2 "waiting for the init server to be fully stopped..."

  # Wait until either the last line of the log contains "server drained and
  # shutdown completed" or timeout.
  until [[ $"TIME_COUNTER" -eq $"TIMEOUT" ]] || \
        [[ "$(tail -1 $LOG_PATH)" == *"server drained and shutdown completed"* ]];
  do
    sleep 2
    ((TIME_COUNTER+=1))
  done

  # If timeout, exit the program.
  if [[ $TIME_COUNTER -ge $TIMEOUT ]]; then
     echo >&2 "error: timeout for stopping the init server"
     exit 1
  fi
  echo >&2 "init server fully stopped"
}

# run_single_node process the command if it contains `start-single-node` argument.
run_single_node() {
  # If /cockroach-data is empty, then run the initialization steps.
  if ! [[ $(ls -A cockroach-data) ]]; then
    if [[ -z "$INSECURE" ]]; then
      setup_certs_dir
    fi
    setup_env
    # Start the init server.
    start_init_node "$@"
    setup_db "$@"
    process_init_files /docker-entrypoint-initdb.d/*
    # Stop the init server.
    stop_init_node
    touch init_success
  fi
  # Start the real server.
  exec $COCKROACH_ENTRYPOINT start-single-node "$@"
  # Mark that the server is successfully started.
}


_main() {
  # If there's no argument passed in, return an error.
  mode=${1:?"error: mode unset, can be shell, bash, or cockroach command \
  (start-single-node, sql, etc.)"}
  shift
  parse_command_line "$@"
  case $mode in
    shell)
        exec /bin/sh "$@" ;;
    bash)
        exec /bin/bash "$@" ;;
    start-single-node)
      run_single_node "$@" ;;
    *)
      if [[ -n "$CERTS_DIR" ]] && [[ -z "$URL" ]]; then
        echo >&2 "error: in secure mode, --url must be specified."
        echo >&2 "check https://www.cockroachlabs.com/docs/stable/connection-parameters#connect-using-a-url"
        exit 1
      fi
      exec $COCKROACH_ENTRYPOINT "$mode" "$@" ;;
  esac
}

_main "$@"
