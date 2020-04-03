#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Ensure demo commands are not available in the sql shell"

# Set up the initial cluster.
start_server $argv

# Spawn a sql shell.
spawn $argv sql
set client_spawn_id $spawn_id
eexpect root@

# Ensure the demo command does not work.
send "\\demo shutdown 2\n"
eexpect "\\demo can only be run with cockroach demo"

# Exit the shell.
interrupt
eexpect eof

# Have good manners and clean up.
stop_server $argv

end_test
