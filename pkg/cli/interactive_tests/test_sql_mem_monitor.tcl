#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# This test ensures that the memory monitor does its main job, namely
# prevent the server from dying because of lack of memory when a
# client runs a "large" query.
# To test this 4 steps are needed:
# 1. a baseline memory usage is measured;
# 2. memory is limited using 'ulimit', and the server restarted with
#    that limit;
# 3. a first test ensure that the server does indeed crash when memory
#    consumption is not limited by a monitor;
# 4. the monitor is configured with a limit and a 2nd test ensures
#    that the server does not crash any more.
# Note that step 3 is needed so as to ensure that the mechanism used
# in step 4 does indeed push memory consumption past the limit.

# Set up the initial cluster.
start_server $argv
stop_server $argv

# Start the cluster anew. This ensures fresh memory state.
start_server $argv

# Make some initial request to check the data is there and define the
# baseline memory consumption.
system "echo 'select * from system.information_schema.columns;' | $argv sql >/dev/null"

# What memory is currently consumed by the server?
set vmem [ exec ps -o vsz= -p [ exec cat server_pid ] ]

# Now play. First, shut down the running server.
stop_server $argv

# Spawn a shell, so we get access to 'ulimit'.
spawn /bin/bash
set shell_spawn_id $spawn_id
send "PS1=':''/# '\r"
eexpect ":/# "

# Set the max memory usage to the baseline plus some margin.
send "ulimit -v [ expr {3*$vmem/2} ]\r"
eexpect ":/# "

# Start a server with this limit set.
send "$argv start-single-node --insecure --max-sql-memory=25% -s=path=logs/db --background --pid-file=server_pid\r"
eexpect ":/# "
send "$argv sql --insecure -e 'select 1'\r"
eexpect "1 row"
eexpect ":/# "
send "tail -F logs/db/logs/cockroach-stderr.log\r"
eexpect "stderr capture started"

# Spawn a client.
spawn $argv sql
set client_spawn_id $spawn_id
eexpect root@

# Test the client is sane.
send "select 1;\r"
eexpect "1 row"
eexpect root@

start_test "Ensure that memory over-allocation without monitoring crashes the server"
# Now try to run a large-ish query on the client.
# The query is a 4-way cross-join on information_schema.columns,
# resulting in ~8 million rows loaded into memory when run on an
# empty database.
send "set database=system;\r"
eexpect root@
# Disable query distribution to force in-memory computation.
send "set distsql=off;\r"
eexpect SET
send "with a as (select * from generate_series(1,10000000)) select * from a as a, a as b, a as c, a as d limit 10;\r"
eexpect "connection lost"

# Check that the query crashed the server
set spawn_id $shell_spawn_id
# Error is either "out of memory" (Go) or "cannot allocate memory" (C++)
expect {
    "out of memory" {}
    "cannot allocate memory" {}
    "std::bad_alloc" {}
    "Resource temporarily unavailable" {}
    timeout { handle_timeout "memory allocation error" }
}
# Stop the tail command.
interrupt
eexpect ":/# "

# Check that the client got a bad connection error
set spawn_id $client_spawn_id
eexpect root@
end_test

start_test "Ensure that memory monitoring prevents crashes"
# Re-launch a server with relatively lower limit for SQL memory
set spawn_id $shell_spawn_id
send "$argv start-single-node --insecure --max-sql-memory=1000K -s=path=logs/db \r"
eexpect "restarted pre-existing node"
sleep 2

# Now try the large query again.
set spawn_id $client_spawn_id
send "select 1;\r"
eexpect root@
send "set database=system;\r"
eexpect root@
send "with a as (select * from generate_series(1,100000)) select * from a as a, a as b, a as c, a as d limit 10;\r"
eexpect "memory budget exceeded"
eexpect root@

# Check we can send another query without error -- the server has survived.
send "select 1;\r"
eexpect "1 row"
eexpect root@
end_test

send_eof
eexpect eof

set spawn_id $shell_spawn_id
interrupt
interrupt
eexpect ":/# "
send "exit\r"
eexpect eof
