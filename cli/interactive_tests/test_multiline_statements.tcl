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

# Send up-arrow.
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

# Send up-arrow.
send "\033\[A"
eexpect "SELECT 1, 2, 3;"

# Test that Ctrl+C after the first line merely cancels the statement and presents the prompt.
send "\r"
eexpect root@
send "SELECT\r"
eexpect " ->"
send "\003"
eexpect root@

send "\003"
eexpect eof

stop_server $argv
