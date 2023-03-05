#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

start_test "check \s prints out command history"
send "select 1;\r"
eexpect "1 row"
send "\\s\r"
eexpect "select 1;\r"
end_test
