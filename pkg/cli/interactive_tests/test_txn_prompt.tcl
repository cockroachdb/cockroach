#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

send "$argv sql\r"
eexpect root@

# Create test database, test database prompt.
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

# Test that prompt becomes "OPEN>"
send "BEGIN;\r\r"

eexpect "\nBEGIN\r\n"
eexpect root@
eexpect "OPEN>"

# Test that prompt becomes "ERROR>"
send "select a;\r"
eexpect "pq: column name \"a\""
eexpect root@
eexpect "ERROR>"

# Test that prompt becomes "DONE>"
send "ROLLBACK;\r"
eexpect "\nROLLBACK\r\n"
eexpect root@

send "BEGIN; SAVEPOINT cockroach_restart;\r\r"
eexpect OK
eexpect root@
send "SELECT 1;\r"
eexpect "1 row"
eexpect root@
send "RELEASE SAVEPOINT cockroach_restart;\r"
eexpect "\nCOMMIT\r\n"
eexpect root@
eexpect "DONE>"

# Test that prompt becomes "RETRY>"
send "COMMIT;\r"
eexpect root@

send "BEGIN; SAVEPOINT cockroach_restart;\r\r"
eexpect OK
eexpect root@
send "SELECT CRDB_INTERNAL.FORCE_RETRY('1s':::INTERVAL);\r"
eexpect "pq: restart transaction"
eexpect root@
eexpect "RETRY>"

send "ROLLBACK TO SAVEPOINT cockroach_restart;\r"
eexpect OK
eexpect root@
eexpect "OPEN>"

send "COMMIT;\r"
eexpect root@

# Test that prompt becomes " ?>"
stop_server $argv

send "SELECT 1; SELECT 1;\r"
eexpect "connection lost"
eexpect root@
eexpect " \\?>"

# Terminate.
send "\\q\r"
eexpect ":/# "

send "exit\r"
eexpect eof

