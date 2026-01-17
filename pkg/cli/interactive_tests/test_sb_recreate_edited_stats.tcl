#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

proc file_exists {filepath} {
  if {! [ file exist $filepath]} {
    report "MISSING EXPECTED FILE: $filepath"
    exit 1
  }
}

start_test "Ensure that 'debug sb recreate' uses edited statistics"

spawn $argv demo --no-line-editor --empty --log-dir=logs
eexpect "defaultdb>"

# Create table and insert data
send "CREATE TABLE t (a INT, INDEX (a));\r"
eexpect "defaultdb>"

send "INSERT INTO t SELECT 100 FROM generate_series(0, 99);\r"
eexpect "defaultdb>"

# Collect statistics on the table
send "ANALYZE t;\r"
eexpect "defaultdb>"

# Create a statement bundle with a query using the statistics
send "EXPLAIN ANALYZE (DEBUG) SELECT * FROM t WHERE a = 100;\r"
eexpect "Statement diagnostics bundle generated."
expect -re "SQL shell: \\\\statement-diag download (\\d+)" {
  set id1 $expect_out(1,string)
}

send "\\statement-diag download $id1\r"
eexpect "Bundle saved to"
eexpect root@

file_exists "stmt-bundle-$id1.zip"

send_eof
eexpect eof

# Extract the bundle
set python "python2.7"
set pyfile [file join [file dirname $argv0] unzip.py]
system "mkdir bundle"
system "$python $pyfile stmt-bundle-$id1.zip bundle"

# Back up the original stats file
system "cp bundle/stats-defaultdb.public.t.sql bundle/stats-defaultdb.public.t.sql~"

# Edit the histogram to change a=100 to a=200 in the JSON
# This simulates a user manually editing stats to test different scenarios
system "sed -i.bak 's/\"upper_bound\": \"100\"/\"upper_bound\": \"200\"/g' bundle/stats-defaultdb.public.t.sql"

# Recreate the bundle with edited stats
spawn $argv debug sb recreate bundle
eexpect "Statement was:"
eexpect "SELECT"
eexpect "defaultdb>"

# Verify the statistics were loaded by checking SHOW STATISTICS
send "SELECT statistics_name, column_names, row_count, distinct_count FROM [SHOW STATISTICS FOR TABLE t];\r"
eexpect "100"
eexpect "defaultdb>"

# Verify that the edited stats are used by running EXPLAIN queries
# The optimizer should now think the data has a=200 instead of a=100
send "EXPLAIN SELECT * FROM t WHERE a = 100;\r"
eexpect "estimated row count: 1"
eexpect "defaultdb>"

send "EXPLAIN SELECT * FROM t WHERE a = 200;\r"
eexpect "estimated row count: 100"
eexpect "defaultdb>"

send_eof
eexpect eof

end_test
