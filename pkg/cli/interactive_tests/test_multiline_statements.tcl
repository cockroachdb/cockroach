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

# Test that \show does what it says.
send "\\show\r"
eexpect "select 1,\r\n2, 3\r\n*->"

# Test finishing the multi-line statement.
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
interrupt
eexpect root@

# Test that BEGIN .. without COMMIT begins a multi-line statement.
send "BEGIN; SELECT 1;\r"
eexpect " ->"

# Test that \show does what it says.
send "\\show\r"
eexpect "BEGIN*;\r\nSELECT 1;\r\n*->"

# Test that a COMMIT is detected properly.
send "COMMIT;\r"
eexpect "1 row"
eexpect "root@"

# Test that an invalid statement inside a multi-line txn doesn't go to the
# server.
send "BEGIN;\r"
eexpect " ->"
send "SELEC T1;\r"
eexpect "invalid syntax"
eexpect " ->"
send "SELECT 1; COMMIT;\r"
eexpect "1 row"
eexpect root@

interrupt
eexpect eof

stop_server $argv
