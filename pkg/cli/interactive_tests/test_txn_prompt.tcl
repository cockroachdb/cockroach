#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

send "$argv sql\r"
eexpect root@

start_test "Check option to echo statements"
send "\\set echo\r"
send "select 1;\r"
eexpect "\n> select 1;\r\n"
eexpect root@
end_test

start_test "Check prompt update statements are disabled when smart_prompt is not set"
send "\r"
eexpect "> SHOW TRANSACTION STATUS"
eexpect root@
send "\\unset smart_prompt\r"
send "set application_name=test;\r"
eexpect "> set application_name=test;"
eexpect "SET"
expect {
    "SHOW" {
	report "unexpected SHOW"
	exit 1
    }
    root@ {}
}

# reset settings for later tests
send "\\unset echo\r"
send "\\set smart_prompt\r"
end_test

start_test "Check database prompt."
send "CREATE DATABASE IF NOT EXISTS testdb;\r"
eexpect "\nCREATE DATABASE\r\n"
eexpect root@
send "SET DATABASE = testdb;\r"
eexpect "\nSET\r\n"
eexpect root@
eexpect "/testdb>"
send "SET DATABASE = '';\r"
eexpect "\nSET\r\n"
eexpect root@
eexpect "/>"
end_test

start_test "Test that prompt becomes OPEN when txn is opened."
send "BEGIN;\r\r"

eexpect "\nBEGIN\r\n"
eexpect root@
eexpect "OPEN>"
end_test

start_test "Test that prompt becomes ERROR upon txn error."
send "select a;\r"
eexpect "pq: column name \"a\""
eexpect root@
eexpect "ERROR>"
end_test

start_test "Test that prompt becomes DONE after successful retry attempt."
send "ROLLBACK;\r"
eexpect "\nROLLBACK\r\n"
eexpect root@

send "BEGIN; SAVEPOINT cockroach_restart;\r\r"
eexpect SAVEPOINT
eexpect root@
send "SELECT 1;\r"
eexpect "1 row"
eexpect root@
send "RELEASE SAVEPOINT cockroach_restart;\r"
eexpect "\nCOMMIT\r\n"
eexpect root@
eexpect "DONE>"
end_test

start_test "Test that prompt becomes RETRY upon retry error."
send "COMMIT;\r"
eexpect root@

send "BEGIN; SAVEPOINT cockroach_restart;\r\r"
eexpect SAVEPOINT
eexpect root@
send "SELECT CRDB_INTERNAL.FORCE_RETRY('1s':::INTERVAL);\r"
eexpect "pq: restart transaction"
eexpect root@
eexpect "RETRY>"
end_test

start_test "Test that prompt reverts to OPEN at beginning of new attempt."
send "ROLLBACK TO SAVEPOINT cockroach_restart;\r"
eexpect ROLLBACK
eexpect root@
eexpect "OPEN>"
end_test

send "COMMIT;\r"
eexpect root@

start_test "Test that prompt becomes ??? upon server unreachable."
stop_server $argv

send "SELECT 1; SELECT 1;\r"
eexpect "connection lost"
eexpect root@
eexpect " \\?>"
end_test

# Terminate.
send "\\q\r"
eexpect ":/# "

send "exit\r"
eexpect eof

