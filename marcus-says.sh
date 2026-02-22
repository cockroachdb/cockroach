#!/bin/bash
set -euo pipefail

# Marcus Says Game
# This script starts a CockroachDB node, creates a random table,
# generates a random query, and prints both.

# Temporary files and directories
STORE_DIR=$(mktemp -d)
RANDOM_PORT=$((26257 + RANDOM % 1000))

# Cleanup function
cleanup() {
    echo
    echo "Cleaning up..."
    ./cockroach quit --insecure --port="$RANDOM_PORT" >/dev/null 2>&1 || true
    sleep 1
    rm -rf "$STORE_DIR"
}
trap cleanup EXIT

echo "=== Marcus Says ==="
echo

# Start cockroach in the background
echo "Starting CockroachDB node..."
./cockroach start-single-node \
    --insecure \
    --background \
    --store="$STORE_DIR" \
    --listen-addr=127.0.0.1:"$RANDOM_PORT" \
    --http-addr=127.0.0.1:0 \
    >/dev/null 2>&1

# Wait for the server to be ready
echo "Waiting for server to be ready..."
URL="postgresql://root@127.0.0.1:$RANDOM_PORT/defaultdb?sslmode=disable&options=-c%20allow_unsafe_internals=true"

for i in {1..30}; do
    if ./cockroach sql --url="$URL" -e "SELECT 1" >/dev/null 2>&1; then
        echo "Server ready!"
        break
    fi
    sleep 1
    if [ $i -eq 30 ]; then
        echo "ERROR: Server failed to start within 30 seconds"
        exit 1
    fi
done

echo

# Generate random CREATE TABLE statements using smith and pick the one with most indexes
echo "Generating random table..."
CREATE_TABLE=$(./bin/smith -num 1000 SimpleNames 2>&1 | \
    awk 'BEGIN { RS=""; ORS="\n\n" }
         /CREATE TABLE/ {
             gsub(/CREATE TABLE ""\./, "CREATE TABLE ");
             print
         }' | \
    awk 'BEGIN { RS="\n\n"; max_indexes=-1; best="" }
         /CREATE TABLE/ {
             # Count occurrences of INDEX
             count = gsub(/INDEX/, "INDEX")
             if (count > max_indexes) {
                 max_indexes = count
                 best = $0
             }
         }
         END {
             gsub(/;$/, "", best)
             gsub(/^--[^\n]*\n/, "", best)
             print best
         }' | grep -v '^--')

# Check if we got a CREATE TABLE statement
if [ -z "$CREATE_TABLE" ]; then
    echo "ERROR: Failed to generate CREATE TABLE statement"
    exit 1
fi

echo "CREATE TABLE statement:"
echo "$CREATE_TABLE;"
echo

# Execute the CREATE TABLE statement
echo "Creating table on cluster..."
if ! echo "$CREATE_TABLE;" | ./cockroach sql --url="$URL" >/dev/null 2>&1; then
    echo "ERROR: Failed to create table"
    echo "Attempting to show error:"
    echo "$CREATE_TABLE;" | ./cockroach sql --url="$URL"
    exit 1
fi

# Give the schema time to propagate
sleep 2

# Extract the table name from the CREATE TABLE statement
TABLE_NAME=$(echo "$CREATE_TABLE" | grep -o 'CREATE TABLE [^ ]*' | awk '{print $3}')

# Generate random SELECT queries until we find one without full scans
echo "Generating random SELECT query without full scans..."
MAX_ATTEMPTS=100
ATTEMPT=0
QUERY_FOUND=false

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    ATTEMPT=$((ATTEMPT + 1))

    # Generate a random query (disable mutations, DDLs, and DO blocks to get SELECT queries)
    RANDOM_QUERY=$(./bin/smith -url="$URL" -num 1 DisableCRDBFns SimpleNames DisableDDLs DisableMutations DisableDoBlocks 2>&1 | \
        awk '/^--/ {next} /^$/ {next} {print}' || true)

    # Skip if query generation failed
    if [ -z "$RANDOM_QUERY" ]; then
        continue
    fi

    # Check if it's a SELECT or WITH query
    if echo "$RANDOM_QUERY" | grep -qiE "^(SELECT|WITH)"; then
        # Try to EXPLAIN it
        EXPLAIN_OUTPUT=$(./cockroach sql --url="$URL" --format=table -e "EXPLAIN $RANDOM_QUERY" 2>&1 || true)

        if [ $? -eq 0 ] && [ -n "$EXPLAIN_OUTPUT" ]; then
            # Check for full scans, norows, and whether the query uses the random table
            if ! echo "$EXPLAIN_OUTPUT" | grep -iq "full scan" && \
               ! echo "$EXPLAIN_OUTPUT" | grep -iq "norows" && \
               echo "$EXPLAIN_OUTPUT" | grep -q "$TABLE_NAME"; then
                QUERY_FOUND=true
                echo "Found SELECT query without full scans after $ATTEMPT attempt(s)"
                break
            fi
        fi
    fi
done

if [ "$QUERY_FOUND" = "false" ]; then
    echo "Warning: Could not find a SELECT query without full scans or norows after $MAX_ATTEMPTS attempts"
    echo "Exiting game."
    exit 1
fi

echo
echo "Random query:"
echo "$RANDOM_QUERY"
echo

# Show the full EXPLAIN output
echo "Query plan:"
FINAL_EXPLAIN_OUTPUT=$(./cockroach sql --url="$URL" --format=table -e "EXPLAIN $RANDOM_QUERY" 2>&1)

if [ $? -eq 0 ]; then
    echo "$FINAL_EXPLAIN_OUTPUT"
    echo

    # Check and report whether there are full scans
    if echo "$FINAL_EXPLAIN_OUTPUT" | grep -iq "full scan"; then
        echo "MARCUS SAYS: FULL SCAN!"
    else
        echo "MARCUS SAYS: PHEW! NO FULL SCANS"
    fi
else
    echo "(query cannot be explained)"
fi
echo

# Loop to allow multiple index drops
while true; do
    # Show current indexes
    echo "Indexes in $TABLE_NAME:"
    echo

    # Query for unique index names and their key columns
    ./cockroach sql --url="$URL" --format=table -e "
    SELECT DISTINCT
        index_name,
        string_agg(column_name || ' ' || direction, ', ' ORDER BY seq_in_index) AS columns
    FROM
        (SELECT * FROM [SHOW INDEXES FROM $TABLE_NAME] WHERE NOT storing)
    GROUP BY
        index_name
    ORDER BY
        index_name;
    " 2>/dev/null

    echo

    # Prompt user to drop an index
    echo "Which index would you like to drop?"
    echo "(Enter the index name, or press Enter to finish)"
    read -r INDEX_TO_DROP

    if [ -z "$INDEX_TO_DROP" ]; then
        break
    fi

    echo
    echo "Dropping index: $INDEX_TO_DROP"

    # Execute DROP INDEX
    DROP_RESULT=$(./cockroach sql --url="$URL" -e "DROP INDEX $TABLE_NAME@$INDEX_TO_DROP" 2>&1 || true)
    DROP_EXIT_CODE=$?

    # Check if we need to use CASCADE
    if echo "$DROP_RESULT" | grep -q "use CASCADE"; then
        echo "$DROP_RESULT"
        echo
        echo "Index is in use as a constraint. Retrying with CASCADE..."

        # Retry with CASCADE
        DROP_RESULT=$(./cockroach sql --url="$URL" -e "DROP INDEX $TABLE_NAME@$INDEX_TO_DROP CASCADE" 2>&1 || true)
        DROP_EXIT_CODE=$?

        if [ -n "$DROP_RESULT" ]; then
            echo "$DROP_RESULT"
        fi
    # Check if we need to handle primary index specially
    elif echo "$DROP_RESULT" | grep -q "cannot drop the primary index of a table using DROP INDEX"; then
        echo "$DROP_RESULT"
        echo
        echo "This is the primary index. Replacing it with a rowid-based primary key..."

        # Add rowid column and change primary key
        ALTER_RESULT=$(./cockroach sql --url="$URL" -e "
            ALTER TABLE $TABLE_NAME ADD COLUMN rowid INT8 NOT NULL DEFAULT unique_rowid();
            ALTER TABLE $TABLE_NAME ALTER PRIMARY KEY USING COLUMNS (rowid);
        " 2>&1 || true)

        if [ $? -eq 0 ]; then
            echo "Primary key replaced successfully!"
            echo
            echo "Now dropping the old primary key (now a secondary index): $INDEX_TO_DROP"

            # Now drop the old primary key which is now a secondary index
            DROP_OLD_PK=$(./cockroach sql --url="$URL" -e "DROP INDEX $TABLE_NAME@$INDEX_TO_DROP" 2>&1 || true)

            if [ $? -eq 0 ]; then
                echo "Old primary key index dropped successfully!"
                DROP_EXIT_CODE=0
            else
                echo "Failed to drop old primary key index:"
                echo "$DROP_OLD_PK"
                DROP_EXIT_CODE=1
            fi
        else
            echo "Failed to replace primary key:"
            echo "$ALTER_RESULT"
            DROP_EXIT_CODE=1
        fi
    else
        # Show the database output for normal DROP INDEX
        if [ -n "$DROP_RESULT" ]; then
            echo "$DROP_RESULT"
        fi
    fi

    if [ $DROP_EXIT_CODE -eq 0 ]; then
        echo

        # Give the schema change time to propagate
        sleep 2

        # Re-run EXPLAIN to show the impact
        echo "Query plan after dropping index:"
        NEW_EXPLAIN_OUTPUT=$(./cockroach sql --url="$URL" --format=table -e "EXPLAIN $RANDOM_QUERY" 2>&1 || true)
        EXPLAIN_EXIT_CODE=$?

        if [ $EXPLAIN_EXIT_CODE -eq 0 ]; then
            echo "$NEW_EXPLAIN_OUTPUT"
            echo

            # Check and report whether there are full scans now
            if echo "$NEW_EXPLAIN_OUTPUT" | grep -iq "full scan"; then
                echo "MARCUS SAYS: FULL SCAN!"
                break
            else
                echo "MARCUS SAYS: PHEW! NO FULL SCANS"
            fi
        else
            echo "(query cannot be explained after index drop)"
        fi
        echo
    fi
done

echo "=== Game Complete ==="
