#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

# Test that a multi-line entry can be recalled escaped.
send "select 'foo\r"
eexpect " ->"
send "bar';\r"
eexpect "1 row"
eexpect "root@"

send "\033\[A"
eexpect "SELECT e'foo\\\\nbar';"
send "\r"
eexpect "root@"

send "select 1,\r"
eexpect " ->"
send "2, 3\r"
eexpect " ->"
send ";\r"
eexpect "1 row"
eexpect "root@"

send "\033\[A"
eexpect "SELECT 1, 2, 3;"

send "\003"
eexpect eof

stop_server $argv
