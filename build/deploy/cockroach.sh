#!/bin/bash
set -eu

# REGEX-es are used to check if the command contains certain flags, and to extract
# value assigned to a certain flag.
START_SINGLE_NODE_REGEX="(^|[[:space:]])(start-single-node)($|[[:space:]])"
INSECURE_MODE_REGEX="(^|[[:space:]])(--insecure)($|[[:space:]])"
LISTEN_ADDR_REGEX="(^|[[:space:]])(--listen-addr=([^[:space:]]+))($|[[:space:]])"
URL_REGEX="(^|[[:space:]])(--url=([^[:space:]]+))($|[[:space:]])"
CERTSDIR_REGEX="(^|[[:space:]])(--certs-dir=([^[:space:]]+))($|[[:space:]])"

COCKROACH_ENTRYPOINT="/cockroach/cockroach"

# Default values for certain flags. If the given command is not specified with
# any of these flags, assign the default value to it.
LISTEN_ADDR=127.0.0.1
CERTS_DIR=certs

#TODO(janexing): rename the function
# analyze_command is to extract the values assigned to --listen-addr and --certs-dir flags
# in the command. If any flag is not set, set it to the default value.
analyze_command() {
    if ! [[ "$*" =~ $LISTEN_ADDR_REGEX ]]; then
      set -- $* "--listen-addr=$LISTEN_ADDR"
    else
      # If --listen-addr is set, extract the value and save it in LISTEN_ADDR.
      LISTEN_ADDR=${BASH_REMATCH[3]}
    fi

    if ! [[ "$*" =~ $INSECURE_MODE_REGEX ]]; then
      if ! [[ "$*" =~ $CERTSDIR_REGEX ]]; then
        # If neither --insecure nor --certs-dir is specified, use --insecure mode.
        set -- $* "--insecure"
      else
        # If --certs-dir is set, extract the value and save it in CERTS_DIR.
        CERTS_DIR=${BASH_REMATCH[3]}
      fi
    fi
}

setup_certs_dir() {
  # Create a certificate and key for the current node.
  $COCKROACH_ENTRYPOINT cert create-node --certs-dir=certs --ca-key=certs/ca.key $LISTEN_ADDR

  # If user specifies a cert dir not named "certs", create a dir with the given name
  # and copy files from the default `certs` dir to the new dir.
  if [[ $CERTS_DIR != "certs" ]]; then
    mkdir -p "./$CERTS_DIR"
    mv ./certs/* "./$CERTS_DIR/"
    rm -rf ./certs
  fi
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
  set_env_var "COCKROACH_USER" "$COCKROACH_DATABASE"
  set_env_var "COCKROACH_PASSWORD"
}

# run_sql_query is a helper function to run sql queries.
run_sql_query() {
  local sql_entrypoint="$COCKROACH_ENTRYPOINT sql --url=$(cat server.url)"
  $sql_entrypoint "$@"
}

# create_defaultdb is to create a default database. If there doesn't exist a database
# with the given name, create a new database. Otherwise, no-op.
create_defaultdb() {
  defaultdb_name=$1
  # Check if this database already exists.
  dbAlreadyExists="$(run_sql_query -e "SELECT 1 FROM [SHOW DATABASES] WHERE database_name='$defaultdb_name'")"
  # If there's no database with the given name, dbAlreadyExists should have only 1 row.
  # Otherwise, it should have at lease 2 rows.
  if [ "$(echo -n "$dbAlreadyExists" | grep -c '^')" -eq 1 ]; then
    # Create a database with the given name.
    run_sql_query -e "CREATE DATABASE $defaultdb_name"
    echo "Finished creating default database $defaultdb_name"
  else
    echo "$defaultdb_name already exists"
    fi
}

# process_init_files run all the init scripts from /docker-entrypoint-initdb.d.
# This is largely based on PostgreSQL's docker-entrypoint.sh:
# https://github.com/docker-library/postgres/blob/master/14/alpine/docker-entrypoint.sh#L144-L173
#
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
			*.sql.xz) echo "$0: running $f"; xz --decompress --stdout "$f" | run_sql_query; echo ;;
			*)        echo "$0: ignoring $f" ;;
		esac
		echo
	done
}

# setup_db is to create a default database, create a default user (with password if applicable),
# and grant privileges to this user by running `cockroach sql` queries.
setup_db() {
    create_defaultdb $COCKROACH_DATABASE

    local init_env_query=( )

    local create_user_query="CREATE USER $COCKROACH_USER"

    # If COCKROACH_PASSWORD is not empty, create this default user with given password.
    if [[ ! -z "$COCKROACH_PASSWORD" ]]; then
          create_user_query+="WITH PASSWORD $COCKROACH_PASSWORD"
    fi

    init_env_query+=( -e "$create_user_query" \
                      -e "GRANT ALL ON DATABASE $COCKROACH_DATABASE TO $COCKROACH_USER" \
                      -e "GRANT admin TO $COCKROACH_USER " )

    # Check if the given command contains the `--insecure` flag.
    # If not, add the `--certs-dir=certs` to init_env_query.
    if [[ "$*" =~ $INSECURE_MODE_REGEX ]]; then
              init_env_query+=( --insecure )
              else
              init_env_query+=( "--certs-dir=$CERTS_DIR" )
    fi

    run_sql_query "${init_env_query[@]}"
}

# start_init_node is to start the single node for the initialization.
start_init_node() {
  echo "Starting node for the initialization process. This could take couple seconds"
  rm -f server_fifo; mkfifo server_fifo
  local start_node_query=( $COCKROACH_ENTRYPOINT start-single-node \
                           --background --listening-url-file=server_fifo \
                           --pid-file=server_pid )

  # Check if the given command contains the `--insecure` flag.
  # If not, add the `--certs-dir=certs` to init_env_query.
  if [[ "$*" =~ $INSECURE_MODE_REGEX ]]; then
    start_node_query+=( --insecure )
    else
    start_node_query+=( "--certs-dir=$CERTS_DIR" )
  fi

  # Get the given listening address.
  [[ "$*" =~ $LISTEN_ADDR_REGEX ]] && start_node_query+=( $BASH_REMATCH )

  # Start the node and run in the background.
  "${start_node_query[@]}" &

  # Set a 5-minute timeout for the single node starting up.
  timeout 3000s cat server_fifo>server.url
  exit_status=$?
  if [[ $exit_status -eq 124 ]]; then
      echo >&2 "error: timeout for cockroach init process"
      exit 1
  fi
}

# stop_init_node is to stop the single node for the initialization.
stop_init_node() {
    kill $(cat server_pid)
    while ! kill -0 $(cat server_pid); do
        echo "finishing cockroach init process"
        sleep 3
    done

    sleep 5s
    echo "cockroach init process finished, restart the server now"
}

_main() {
  if [ "${1-}" = "shell" ]; then
    shift
    exec /bin/sh "$@"
  else
    shopt -s nocasematch
    if [[ "$*" =~ $START_SINGLE_NODE_REGEX ]]; then
      echo "start init process"
      STRING_TO_REMOVE=`echo ${BASH_REMATCH//[[:blank:]]/}`
      set -- ${*//$STRING_TO_REMOVE/}

      # If the cockroach-data directory is empty.
      if ! [ "$(ls -A cockroach-data)" ]; then
        analyze_command "$*"

        if [[ "$*" =~ $CERTSDIR_REGEX ]]; then
          setup_certs_dir
        fi

        setup_env
        start_init_node "$*"
        setup_db "$*"
        process_init_files /docker-entrypoint-initdb.d/*
        stop_init_node
      fi
      # restart server.
      exec $COCKROACH_ENTRYPOINT start-single-node "$@"
    else
      if [[ "$*" =~ $CERTSDIR_REGEX ]] && ! [[ "$*" =~ $URL_REGEX ]]; then
        echo >&2 "error: in secure mode, --url must be specified. \
        check https://www.cockroachlabs.com/docs/v21.1/connection-parameters#connect-using-a-url"
      fi
      exec $COCKROACH_ENTRYPOINT "$@"
    fi
  fi
}

_main "$@"
