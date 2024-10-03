#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

if [[ $# < 3 || $# > 4 ]]; then
    cat 1>&2 <<EOF
$0 rewrites the given testfixtures statistics file by running SHOW STATISTICS USING JSON
for each table in the specified database.

Note that this tool assumes a cockroach cluster is up and running in insecure mode, the
specified database is already loaded, and statistics have been collected on all tables.

Usage: $0 path/to/cockroach/binary database filename
EOF
    exit 1
fi

COCKROACH_BINARY=$1
DATABASE=$2
FILENAME=$3

tables=($($COCKROACH_BINARY sql --insecure --format=tsv \
 -e "USE $DATABASE; SELECT table_name FROM [SHOW TABLES] ORDER BY table_name;" | tail -n +2))
tables=("${tables[@]:1}")

echo "Writing statistics to $FILENAME"
echo "# Statistics for $DATABASE" > $FILENAME
echo "" >> $FILENAME

for table in "${tables[@]}"
do
    echo "exec-ddl" >> $FILENAME
    echo -n "ALTER TABLE \"$table\" INJECT STATISTICS '" >> $FILENAME
    # This command selects the statistics in JSON form for the current table. The output
    # is passed through the following sed commands before being written to the file:
    # 1. Remove the first line to leave only the JSON value.
    # 2. Replace duplicate double quotes with one double quote.
    # 3. Remove the double quote at the beginning of the JSON.
    # 4. Remove the double quote at the end of the JSON.
    # 5. Append '; to the last line.
    $COCKROACH_BINARY sql --insecure --format=tsv \
     -e "USE $DATABASE; SELECT jsonb_pretty(statistics) FROM [SHOW STATISTICS USING JSON FOR TABLE \"$table\"];" | tail -n +2 \
     | sed '1d' | sed 's/""/"/g' | sed 's/^"\[/\[/g' | sed 's/\]"$/\]/g' | sed '$ s/$/'\'';/' >> $FILENAME
    echo "----" >> $FILENAME
    echo "" >> $FILENAME
done
