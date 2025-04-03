#!/bin/bash
#
# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# set -e: If the command returns a non-zero exit status, exit the shell.
# set -u: errors if an variable is referenced before being set
# set -m: enable job control.
set -eum

cockroach_entrypoint=${COCKROACH:-"/cockroach/cockroach"}

certs_dir="certs"
url=
listen_addr=
advertise_addr=
default_listen_addr_host="127.0.0.1"
default_port="26257"
default_log_dir="./cockroach-data/logs"

advertise_addr_host=$default_listen_addr_host

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
      --certs-dir)          prev=certs_dir ;;
      --certs-dir=*)        certs_dir=$optarg ;;
      --url)                prev=url ;;
      --url=*)              url=$optarg ;;
      --listen-addr)        prev=listen_addr ;;
      --listen-addr=*)      listen_addr=$optarg ;;
      --advertise-addr)     prev=advertise_addr ;;
      --advertise-addr=*)   advertise_addr=$optarg ;;
    esac
  done

  # Check if the listen address passed by command line is with hostname 127.0.0.1
  # or localhost, and with port 26257.
  if [[ -n "$listen_addr" ]]; then
    local hostname=${listen_addr%%:*}
    local port
    # If the value passed with `--listen-addr` contain the char ":", we extract
    # the substring after ":" as the port number.
    if [[ "$listen_addr" =~ ":" ]]; then
      port=${listen_addr##*:}
    fi

    if [[ ( -n "$hostname" ) && ("$hostname" != $default_listen_addr_host ) && ( "$hostname" != "localhost" ) ]]; then
      echo >&2 "error: hostname of listen_addr must be \"$default_listen_addr_host\" or \"localhost\""
      exit 1
    fi
    if [[ ( -n "$port" ) && ( "$port" != $default_port ) ]]; then
      echo >&2 "error: port of listen_addr must be \"$default_port\""
      exit 1
    fi
  fi

  if [[ -n "$advertise_addr" ]]; then
    advertise_addr_host=${advertise_addr%%:*}
  else
    advertise_addr="$default_listen_addr_host:$default_port"
  fi
}

# setup_certs_dir is to set up the certs dir.
setup_certs_dir() {
  mkdir -p "$certs_dir"

  # If there are no files inside $certs_dir, we create keys in it.
  if [[ $(ls -A "$certs_dir" | wc -l) = 0 ]]; then
    $cockroach_entrypoint cert create-ca --certs-dir="$certs_dir" \
    --ca-key="$certs_dir"/ca.key
    $cockroach_entrypoint cert create-client --certs-dir="$certs_dir" \
    --ca-key="$certs_dir"/ca.key root
  fi

  # If there are no files with the name started with "node", we create key
  # for this specific node.
  if ! ls "$certs_dir"/node.* &>/dev/null; then
    $cockroach_entrypoint cert create-node --certs-dir="$certs_dir" \
    --ca-key="$certs_dir"/ca.key "$advertise_addr_host" "$default_listen_addr_host"
  fi

  echo "certificate dir \"$certs_dir\" is successfully set up"
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
  local start_node_query=( exec $cockroach_entrypoint start-single-node \
                           --listening-url-file=server_fifo \
                           --pid-file=server_pid \
                           --advertise-addr="$advertise_addr" \
                           --certs-dir="$certs_dir" \
                           --log="file-defaults: {dir: $default_log_dir}" \
                           "$@" )

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

# setup_db is to create a default database, create a default user (with password
# if applicable), and grant privileges to this user by running `cockroach sql`
# queries.
setup_db() {
  create_defaultdb
  create_default_user

  local init_env_query=( --certs-dir="$certs_dir" )
  if [[ -n "$COCKROACH_USER" ]]; then
    init_env_query+=( -e "GRANT ALL ON DATABASE "$COCKROACH_DATABASE" TO "$COCKROACH_USER"" \
                      -e "GRANT admin TO "$COCKROACH_USER" " )
  fi

  run_sql_query "${init_env_query[@]}"
}

# process_init_files run all the init scripts from /docker-entrypoint-initdb.d.
# This is largely based on PostgreSQL's docker-entrypoint.sh:
# https://github.com/docker-library/postgres/blob/48a0a3600d170eeafa09372ab5af95b7fdc89c23/14/alpine/docker-entrypoint.sh#L153-L182
# usage: process_init_files [file [file [...]]]
# e.g. process_init_files /your_folder/*
process_init_files() {
  echo "start running init files from /docker-entrypoint-initdb.d"
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
  echo "end running init files from /docker-entrypoint-initdb.d"
}

# run_sql_query is a helper function to run sql queries.
run_sql_query() {
  $cockroach_entrypoint sql --url="$(cat server.url)" --database="${COCKROACH_DATABASE}" "$@"
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
  if [[ -z $(db_already_exists "$COCKROACH_DATABASE") ]]; then
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
  if [[ -z $(user_already_exists "$COCKROACH_USER") ]]; then
    $cockroach_entrypoint cert create-client --certs-dir="$certs_dir" \
    --ca-key="$certs_dir"/ca.key "$COCKROACH_USER"
    echo "==== create key for new user $COCKROACH_USER ===="
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

# run_single_node process the command if it contains `start-single-node` argument.
run_single_node() {
  # If /cockroach-data is empty, run the initialization steps.
  if [[ $(ls -A cockroach-data | wc -l) = 0 ]]; then
    setup_certs_dir
    setup_env
    # Start the server.
    start_init_node "$@"
    setup_db "$@"
    process_init_files /docker-entrypoint-initdb.d/*
    # Bring the background server process to the foreground, otherwise the
    # docker container will automatically exit here.
    echo "init_finished" > ./init_success
    fg %1
  else
      exec $cockroach_entrypoint start-single-node \
          --certs-dir="$certs_dir" \
          --advertise-addr=$advertise_addr \
          "$@"
  fi

}

_main() {
  # If there's no argument passed in, return an error.
  mode=${1:?"error: mode unset, can be shell, bash, or cockroach command \
  (start-single-node, sql, etc.)"}
  shift
  case $mode in
    shell)
        exec /bin/sh "$@" ;;
    bash)
        exec /bin/bash "$@" ;;
    start-single-node)
      parse_command_line "$@"
      run_single_node "$@" ;;
    *)
      exec $cockroach_entrypoint "$mode" "$@" ;;
  esac
}

set_env_var "COCKROACH_ARGS"

if [[ -n "$COCKROACH_ARGS" ]]; then
  _main $COCKROACH_ARGS
else
  _main "$@"
fi
