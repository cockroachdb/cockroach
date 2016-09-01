#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Set up the initial cluster.
start_server $argv
stop_server $argv

# Start the cluster anew. This ensures fresh memory state.
start_server $argv

# Make some initial request to check the data is there and define the
# baseline memory consumption.
system "echo 'select * from information_schema.columns;' | $argv sql >/dev/null"

# What memory is currently consumed by the server?
set vmem [ exec ps --no-headers o vsz -p [ exec cat server_pid ] ]

# Now play. First, shut down the running server.
stop_server $argv

# Spawn a shell, so we get access to 'ulimit'.
spawn /bin/bash
set shell_spawn_id $spawn_id
send "PS1=':''/# '\r"
eexpect ":/# "

# Set the max memory usage to the baseline plus some margin.
send "ulimit -v [ expr {int(2*$vmem)} ]\r"
eexpect ":/# "
send "ulimit -a\r"
eexpect ":/# "

# Start a server with this limit set. The server will now run in the foreground.
send "$argv start\r"
eexpect "restarted pre-existing node"
sleep 1

# Spawn a client.
spawn $argv sql
set client_spawn_id $spawn_id
eexpect root@

# Test the client is sane.
send "select 1;\r"
eexpect "1 row"
eexpect root@

# Now try to run a large-ish query on the client.
send "set database=information_schema;\r"
eexpect root@
send "select * from columns as a, columns as b, columns as c, columns as d limit 10;\r"

# Check that the query crashed the server
set spawn_id $shell_spawn_id
eexpect "out of memory"
eexpect ":/# "

# Check that the client got a bad connection error
set spawn_id $client_spawn_id
eexpect "bad connection"
eexpect root@

# Re-launch a server with very low limit for SQL memory
set spawn_id $shell_spawn_id
send "$argv start --max-sql-memory=30K\r"
eexpect "restarted pre-existing node"
sleep 1

# Now try the large query again.
set spawn_id $client_spawn_id
send "set database=information_schema;\r"
eexpect root@
send "select * from columns as a, columns as b, columns as c, columns as d limit 10;\r"
eexpect "memory budget exceeded"
eexpect root@

# Check we can send another query without error -- the server has survived.
send "select 1;\r"
eexpect "1 row"
eexpect root@

# We just terminate, this will kill both server and client.
