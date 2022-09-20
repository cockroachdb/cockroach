#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# This test ensures timing displayed in the CLI works as expected.

spawn $argv demo movr --no-line-editor
eexpect root@

start_test "Test that server execution time and network latency are printed by default."
send "SELECT pg_sleep(0.02) FROM vehicles LIMIT 1;\r"
eexpect "1 row"
eexpect "execution"
eexpect "network"
eexpect root@

# Ditto with multiple statements on one line
send "SELECT * FROM vehicles LIMIT 1; CREATE TABLE t(a int);\r"
eexpect "1 row"
eexpect "Note: timings for multiple statements on a single line are not supported"
eexpect root@
end_test

start_test "Test show_server_execution_times works correctly"
send "\\set show_server_times=false\r"
send "SELECT pg_sleep(0.02) FROM vehicles LIMIT 1;\r"
eexpect "\nTime:"
eexpect root@
send "\\set show_server_times=true\r"
send "SELECT pg_sleep(0.02) FROM vehicles LIMIT 1;\r"
eexpect "execution"
eexpect "network"
eexpect root@
end_test

start_test "Check that server times also work if IntervalStyle is different"
# regression test for issue #67618.
send "set IntervalStyle = 'iso_8601';\r"
eexpect "SET"
eexpect root@
send "SELECT pg_sleep(0.02) FROM vehicles LIMIT 1;\r"
eexpect "execution"
eexpect "network"
eexpect root@

send "set IntervalStyle = 'sql_standard';\r"
eexpect "SET"
eexpect root@
send "SELECT pg_sleep(0.02) FROM vehicles LIMIT 1;\r"
eexpect "execution"
eexpect "network"
eexpect root@
end_test

start_test "Test non-negative PREPARE stmt timings #54888"
send "create table t1 (a int, updated_at timestamptz);\r"
send "prepare stmt (timestamptz) as insert into t1 values (1, \$1);\r"
eexpect "execution \[0-9\]"
eexpect "network \[0-9\]"
eexpect root@
end_test

start_test "Test observer statements non neg timings #54750"
send "SHOW SYNTAX 'CREATE TABLE t(a INT)';\r"
eexpect "execution \[0-9\]"
eexpect "network \[0-9\]"
eexpect root@
end_test
