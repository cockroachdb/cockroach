#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Set a larger timeout since we are talking to every node with a delay.
set timeout 90

start_test "Check --global flag runs as expected"

# Start a demo with --global set
spawn $argv demo --no-example-database --nodes 9 --global

# Ensure db is defaultdb.
eexpect "defaultdb>"

# Ensure regions display correctly.
send "SELECT region, zones FROM \[SHOW REGIONS FROM CLUSTER\] ORDER BY region;\r"
eexpect "  europe-west1 | {b,c,d}"
eexpect "  us-east1     | {b,c,d}"
eexpect "  us-west1     | {a,b,c}"

# Test we cannot add or restart nodes.
send "\\demo add region=europe-west1\r"
eexpect "adding nodes is not supported in --global configurations"
eexpect "defaultdb>"

send "\\demo shutdown 3\r"
eexpect "shutting down nodes is not supported in --global configurations"
eexpect "defaultdb>"

interrupt
eexpect eof
end_test
