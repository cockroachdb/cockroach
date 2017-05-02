#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

send "select 1;\r"
eexpect "1 row"
eexpect root@

# Check that the client properly detects the server went down.
# We need to force since the open connection may prevent a quick
# graceful shutdown.
force_stop_server $argv

send "select 1;\r"
eexpect "bad connection"
eexpect root@

send "select 1;\r"
eexpect "opening new connection"
expect {
    "connection refused" {}
    "connection reset by peer" {}
    timeout { handle_timeout "connection error" }
}
eexpect root@

# Check that the client automatically reconnects when the server goes up again.
start_server $argv

send "select 1;\r"
eexpect "opening new connection"
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

stop_server $argv
