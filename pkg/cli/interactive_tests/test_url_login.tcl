#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

set ::env(COCKROACH_URL) "postgresql://localhost:26257?sslmode=disable"

# Check that the client can start when no username is specified.
spawn $argv sql
eexpect @localhost

send "\003"
eexpect eof

stop_server $argv
