#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Set a larger timeout since we are going global.
set timeout 90

start_test "Check --global flag runs as expected"

# Start a demo with --global set
spawn $argv demo movr --nodes 9 --global

# Ensure db is movr.
eexpect "movr>"

# Expect queries to work.
send "SELECT count(*) FROM movr.rides;\r"
eexpect "500"
eexpect "movr>"

interrupt
eexpect eof
end_test
