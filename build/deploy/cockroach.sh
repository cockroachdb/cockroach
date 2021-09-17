#!/bin/bash
set -eu

defaultdb_file=.cockroach_defaultdb

# create_defaultdb is to create a default database.
# Use database name saved in .cockroach_defaultdb. This name is specified by the
# environment variable COCKROACH_DB. It should only be called if .cockroach_defaultdb
# exists and is not empty.
create_defaultdb() {
  # Load the name of default database from .cockroach_defaultdb.
  defaultdb_name=$(<$defaultdb_file)
  # Check if this database already exists.
  dbAlreadyExists="$("$@" -e "SELECT 1 FROM [SHOW DATABASES] WHERE database_name='$defaultdb_name'")"
  # If there's no database with this name, dbAlreadyExists should have only 1 row.
  # Otherwise, it should have at lease 2 rows.
  if [ "$(echo -n "$dbAlreadyExists" | grep -c '^')" -eq 1 ]; then
    # Create a database with defaultdb_name.
    "$@" -e "CREATE DATABASE $defaultdb_name"
    echo "Finished creating default database $defaultdb_name"
  else
    echo "$defaultdb_name already exists"
    fi
  # Delete .cockroach_defaultdb, so that following sql commands won't create a default database
  # again.
  rm $defaultdb_file
}

# create_defaultdb_file is to save the default database name into .cockroach_defaultdb.
create_defaultdb_file(){
  local defaultdb_name=$1
  if [ ! -s "$defaultdb_file" ]; then
    echo $1 >> "$defaultdb_file"
    echo "saved the name of default database into $defaultdb_file"
    fi
}

# set_env_var is to set an environment variable with value.
# The first argument is the name of environment variable,
# and the second argument is the value.
# If the value argument is unset, it's null by default.
# If this environment variable is already assigned a value,
# we don't override, and keep the original value.
# usage: set_env_var VAR [VAL]
set_env_var() {
  local env_var="$1"
  local val="${2:-}"
  if [ "${!env_var:-}" ]; then
    val="${!env_var}"
  fi
  export "$env_var"="$val"
}

# set_env_var is to set up the environment values,
# and save the name of default database to .cockroach_defaultdb.
# It should only be used when `docker init`.
setup_env() {
  set_env_var "COCKROACH_DB" "defaultdb"
  create_defaultdb_file $COCKROACH_DB
}

_main() {
  local exec_command

  if [ "${1-}" = "shell" ]; then
    shift
    exec_command=( /bin/sh "$@" )
  else
    exec_command=( /cockroach/cockroach "$@")
  fi

  # If the command is "docker init", save the env var `COCKROACH_DB`
  # to .cockroach_defaultdb.
  if [[ "${1-}" = "init" ]]; then
    setup_env
  # If the commmand is "docker sql", and .cockroach_defaultdb is non-empty,
  # create a default database with name from .cockroach_defaultdb.
  elif [[ "${1-}" = "sql" && -s $defaultdb_file ]]; then
    create_defaultdb "${exec_command[@]}"
  fi

  eval "${exec_command[@]}"
}

_main "$@"
