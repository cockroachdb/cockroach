#!/usr/bin/env bash
# Usage Example: ./create-backup.sh 24.1.3

VERSION="${1?}"

function create_backup {
	NAME=${1?}
	BACKUP_DIR="`pwd`/${NAME?}-restore/${VERSION?}"

	echo "creating ${BACKUP_DIR?}" 

	rm -rf "${BACKUP_DIR?}"

	roachprod destroy local
	roachprod create local -n 1
	roachprod stage local release v${VERSION?}
	roachprod start local

	# run the backup script as root because assertions expect objects to be owned
	# by root.
	roachprod sql local:1 --auth-mode=root -- -e "$(cat create-${1?}.sql)"

	roachprod sql local:1 -- -e "BACKUP INTO 'nodelocal://1/backup';"

	mv "$HOME/local/1/data/extern/backup" "${BACKUP_DIR?}"
}

create_backup cluster
create_backup system-role-members
create_backup system-privileges
create_backup system-database-role-settings
create_backup system-external-connections
