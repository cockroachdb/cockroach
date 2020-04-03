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
send "\\demo_node shutdown 2\n"
eexpect "\\demo_node can only be run with cockroach demo"

# Ensure rand_fill_table does not work.
send "\\rand_fill_table t 1\n"
eexpect "\\rand_fill_table can only be run with cockroach demo"

# Exit the shell.
interrupt
eexpect eof

# Have good manners and clean up.
stop_server $argv

end_test
