#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

send "$argv sql --host=localhost\r"
eexpect root@

###START tests prompt customization

start_test "Check that invalid prompt patterns cause an error."
send "\\set prompt1 %?\r"
eexpect "unrecognized format code in prompt"

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test

start_test "Check that one can use % signs in the prompt."
send "\\set prompt1 abc%%def\r"
eexpect "abc%def"

send "\\set prompt1 abc%Mdef\r"
eexpect abclocalhost:26257def

send "\\set prompt1 abc%mdef\r"
eexpect abclocalhostdef

send "\\set prompt1 abc%>def\r"
eexpect abc26257def

send "\\set prompt1 abc%ndef\r"
eexpect abcrootdef

send "\\set prompt1 abc%/def\r"
eexpect abcdefaultdbdef

# Check promptw with no formatting code.
send "\\set prompt1 woo \r"
eexpect woo

send "SET database \r"
eexpect "\r\n -> "
send "defaultdb;\r"
eexpect woo

# Reset to default.
send "\\unset prompt1\r"
eexpect root@
end_test

start_test "Check option to echo statements"
send "\\set echo\r"
send "select 1;\r"
eexpect "\n> select 1;\r\n"
eexpect root@
end_test

start_test "Check database prompt."
send "CREATE DATABASE IF NOT EXISTS testdb;\r"
eexpect "\nCREATE DATABASE\r\n"
eexpect root@
send "SET DATABASE = testdb;\r"
eexpect "\nSET\r\n"
eexpect root@
eexpect "/testdb>"
send "SET sql_safe_updates = false;\r"
eexpect "\nSET\r\n"
send "SET database = '';\r"
eexpect "\nSET\r\n"
eexpect root@
eexpect "/>"
send "SET database = 'defaultdb';\r"
eexpect "\nSET\r\n"
eexpect root@
end_test

start_test "Test that prompt becomes OPEN when txn is opened."
send "BEGIN;\r\r"

eexpect "\nBEGIN\r\n"
eexpect root@
eexpect "OPEN>"
end_test

start_test "Test that prompt becomes ERROR upon txn error."
send "select a;\r"
eexpect "ERROR: column \"a\" does not exist"
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
send "SELECT crdb_internal.force_retry('1s':::INTERVAL);\r"
eexpect "ERROR: restart transaction"
eexpect root@
eexpect "ERROR>"
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
