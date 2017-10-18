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
eexpect "select 'foo"
eexpect "\rbar';"
send "\r"
eexpect "root@"

send "select 1,\r"
eexpect " ->"
send "2, 3\r"
eexpect " ->"
end_test

start_test "Test that \show does what it says."
send "\\show\r"
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

start_test "Test that Ctrl+C after the first line merely cancels the statement and presents the prompt."
send "\r"
eexpect root@
send "select\r"
eexpect " ->"
interrupt
eexpect root@
end_test

start_test "Test that BEGIN .. without COMMIT begins a multi-line statement."
send "begin; select 1;\r"
eexpect " ->"
end_test

start_test "Test that \show does what it says."
send "\\show\r"
eexpect "begin; select 1;\r\n*->"
end_test

start_test "Test that a COMMIT is detected properly."
send "commit;\r"
eexpect "1 row"
eexpect "root@"
end_test

start_test "Test that BEGIN .. without COMMIT also begins a multi-line statement with smart_prompt disabled."
# Issue #19219.
send "\\unset smart_prompt\r"
send "begin; select 1;\r"
eexpect " ->"
send "commit;\r"
eexpect root@
send "begin; select 1;\r"
eexpect " ->"
send "commit;\r"
eexpect "root@"
send "\\set smart_prompt\r"
end_test

start_test "Test that BEGIN .. without COMMIT does not begin a multi-line statement in open txns. #16833"

# trigger the error state
send "begin; select nonexistent;\r\r"
eexpect "not found"
eexpect ERROR

# Try to send a txn prefix, expect no multiline entry
send "begin; select 1;\r"
eexpect "commands ignored"
eexpect "root@"

# clear status for next test
send "commit;\r"
eexpect ROLLBACK
eexpect "root@"
end_test

start_test "Test that an invalid statement inside a multi-line txn does not go to the server."
send "begin;\r"
eexpect " ->"
send "selec t1;\r"
eexpect "invalid syntax"
eexpect " ->"
send "select 1; commit;\r"
eexpect "1 row"
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

interrupt
eexpect eof

stop_server $argv
