#!/usr/bin/env bash

VERSION="24.2.4"

function create_backup {
	NAME=${1?}

	rm -rf "./${NAME?}-restore/${VERSION?}"

	roachprod destroy local
	roachprod create local -n 1
	roachprod stage local release v${VERSION?}
	roachprod start local

	# every backup is expected to have a create-<backup-name>.sql file that is
	# used to construct the backup.
	roachprod sql local:1 -- -e "$(cat pkg/backup/testdata/restore_old_versions/create-${1?}.sql)"
	roachprod sql local:1 -- -e "BACKUP INTO 'nodelocal://1/backup';"

	mv "$HOME/local/1/data/extern/backup" "./pkg/backup/testdata/restore_old_versions/${NAME?}-restore/${VERSION?}"
}

create_backup cluster
create_backup system-role-members
create_backup system-privileges
create_backup system-database-role-settings
create_backup system-external-connections
