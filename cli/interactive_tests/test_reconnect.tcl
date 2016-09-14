#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn $argv sql
eexpect root@

send "select 1;\r"
eexpect "1 row"
eexpect root@

# Check that the client properly detects the server went down.
stop_server $argv

send "select 1;\r"
eexpect "bad connection"
eexpect root@

send "select 1;\r"
eexpect "connection refused"
eexpect root@

# Check that the client automatically reconnects when the server goes up again.
start_server $argv

send "select 1;\r"
eexpect "1 row"
eexpect root@

# Check that the client picks up when the server was restarted.
stop_server $argv
start_server $argv

send "select 1;\r"
eexpect "bad connection"
eexpect root@

send "select 1;\r"
eexpect "1 row"
eexpect root@

send "\\q\r"
eexpect eof
