#!/bin/bash
set -euo pipefail

cd "$(dirname $0)/.."
if [[ $# < 4 || $# > 5 ]]; then
    cat 1>&2 <<EOF
$0 compares plans of newline-separated queries in queryfile by running EXPLAIN
(VERBOSE) <query> (or a user-provided explain variant) against each cockroach
server specified.
Note that this tool assumes the server is up and running in insecure mode.
Usage: $0 path/to/cockroach/binary queryfile oldpgurl newpgurl [explain variant]
EOF
    exit 1
fi

COCKROACH_BINARY=$1
QUERYFILE=$2
OLDPGURL=$3
NEWPGURL=$4
if [[ $# < 5 ]]; then
    EXPLAIN_VARIANT="EXPLAIN (VERBOSE)"
else
    EXPLAIN_VARIANT=$5
fi

dest=$(mktemp -d)
echo "Writing query plans to ${dest}"

pgurls=($OLDPGURL $NEWPGURL)

querynum=0
while read line
do
    for pgurl in "${pgurls[@]}"
    do
        version="old"
        if [[ $pgurl != $OLDPGURL ]]; then
            version="new"
        fi

        # Run the sql client displaying the results in table format for easy
        # to read output. The sed command removes the line "(n rows)" from the
        # output.
        $COCKROACH_BINARY sql --insecure --url=$pgurl --format=table -e "$EXPLAIN_VARIANT $line" | sed '/^(/ d' > $dest/query$querynum.$version
    done

    # Diff the query plans.
    tmpfile=$(mktemp)
    if ! diff "$dest/query$querynum.old" "$dest/query$querynum.new" > $tmpfile; then
        echo "found difference in plans for query $querynum: $line"
        cat $tmpfile
        echo ""
    fi
    rm $tmpfile
    ((querynum++))
done < $QUERYFILE
