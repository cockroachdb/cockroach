#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

start_test "Test that a multi-line entry can be recalled escaped."
send "select 'foo\r"
eexpect " ->"
send "bar';\r"
eexpect "1 row"
eexpect "root@"

# Send up-arrow.
send "\033\[A"
eexpect "select 'foo\r\nbar';"
send "\r"
eexpect "root@"

send "select 1,\r"
eexpect " ->"
send "2, 3\r"
eexpect " ->"
end_test

start_test "Test that \p does what it says."
send "\\p\r"
eexpect "select 1,"
eexpect "2, 3"
eexpect " ->"
end_test

start_test "Test finishing the multi-line statement."
send ";\r"
eexpect "1 row"
eexpect "root@"

# Send up-arrow.
send "\033\[A"
eexpect "select 1,"
eexpect "2, 3"
eexpect ";"
end_test

start_test "Test that \r does what it says."
# backspace to erase the semicolon
send "\010"
# newline to get a prompt
send "\r"
eexpect " ->"
# Now send \r followed by a carriage return.
send "\\r\r"
eexpect root@
end_test

start_test "Test that Ctrl+C after the first line merely cancels the statement and presents the prompt."
send "\r"
eexpect root@
send "select\r"
eexpect " ->"
interrupt
eexpect root@
end_test

start_test "Test that \p does what it says."
send "select\r"
eexpect " ->"
send "\\p\r"
eexpect "select\r\n*->"
interrupt
eexpect root@
end_test

start_test "Test that a dangling table creation can be committed, and that other non-DDL, non-DML statements can be issued in the same txn. (#15283)"
send "create database if not exists t;"
send "drop table if exists t.blih;"
send "create table if not exists t.kv(k int primary key, v int);\r"
eexpect "CREATE TABLE"
eexpect root@
send "begin; create table t.blih(x INT REFERENCES t.kv(k));\r\r"
eexpect "CREATE TABLE"
eexpect root@
eexpect OPEN

send "show all cluster settings;\r"
eexpect "rows"
eexpect root@
eexpect OPEN

send "commit;\r"
eexpect COMMIT
eexpect root@
end_test


send_eof
eexpect eof

stop_server $argv
