#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

# Test that prompt becomes "TXN>"
send "$argv sql\r"
eexpect root@
send "begin;\r\r"
eexpect "begin"
eexpect "TXN>"

# Test that prompt becomes "ERR>"
send "select a;\r"
eexpect "pq: column name \"a\""
eexpect "ERR>"
send "rollback;\r"

# Test that prompt becomes "unknown>"
send "begin;\r\r"
stop_server $argv
send "select 1;\r"
eexpect "unknown>"

send "\\q\r"
eexpect ":/# "
send "exit\r"
eexpect eof

