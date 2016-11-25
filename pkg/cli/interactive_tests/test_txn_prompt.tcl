#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

# Test that prompt becomes "OPEN>"
send "$argv sql\r"
eexpect root@
send "begin;\r\r"
eexpect "begin"
eexpect "OPEN>"

# Test that prompt becomes "ERROR>"
send "select a;\r"
eexpect "pq: column name \"a\""
eexpect "ERROR>"
send "rollback;\r"

# Test that prompt becomes "DONE>"
send "begin;SAVEPOINT cockroach_restart;\r\r"
send "select 1;\r"
send "RELEASE SAVEPOINT cockroach_restart;\r"
eexpect "DONE>"
send "commit;\r"

# Test that prompt becomes "RETRY>"
send "begin;SAVEPOINT cockroach_restart;\r\r"
send "SELECT CRDB_INTERNAL.FORCE_RETRY('1s':::INTERVAL);\r"
eexpect "RETRY>"
send "ROLLBACK TO SAVEPOINT cockroach_restart;\r"
eexpect "OPEN>"
send "commit;\r"

# Test that prompt becomes "?????>"
send "begin;\r\r"
stop_server $argv
send "select 1;\r"
eexpect "?????>"

send "\\q\r"
eexpect ":/# "
send "exit\r"
eexpect eof

