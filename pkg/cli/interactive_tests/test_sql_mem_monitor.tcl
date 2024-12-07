#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# This test ensures that the memory monitor does its main job, namely prevents
# the server from dying because of lack of memory when a client runs a "large"
# query.
# To test this 4 steps are needed:
# 1. a baseline memory usage is measured;
# 2. memory is limited using 'ulimit', and the server restarted with that limit;
# 3. the 1st test ensures that the server does indeed crash when memory
#    consumption is not limited by a monitor;
# 4. the monitor is configured with a limit and the 2nd test ensures that the
#    server does not crash any more - an error is returned instead.
# Note that step 3 is needed so as to ensure that the mechanism used in step 4
# does indeed push memory consumption past the limit.

# Set up the initial cluster.
start_server $argv
stop_server $argv

# Start the cluster anew. This ensures fresh memory state.
start_server $argv

# Make some initial request to check the data is there and define the
# baseline memory consumption.
system "echo 'select count(*) from generate_series(1,10000000);' | $argv sql >/dev/null"

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

# Start a server with this limit set. Note that we're setting max-sql-memory so
# high so that ulimit kills the process before root SQL monitor's limit has been
# consumed.
send "$argv start-single-node --insecure --max-sql-memory=75% -s=path=logs/db --background --pid-file=server_pid\r"
eexpect ":/# "
send "$argv sql --insecure -e 'select 1'\r"
eexpect "1 row"
eexpect ":/# "
send "tail -F logs/db/logs/cockroach-stderr.log\r"
eexpect "stderr capture started"

# Spawn a client.
spawn $argv sql --no-line-editor
set client_spawn_id $spawn_id
eexpect root@

# Test the client is sane.
send "select 1;\r"
eexpect "1 row"
eexpect root@

start_test "Ensure that memory over-allocation without monitoring crashes the server"
# Now try to run a large-ish query on the client. The query is a 4-way cross
# join on top of a CTE with 10M rows.
#
# Disable spilling to disk by giving huge workmem budget to the query
# (importantly, this budget must be larger than vmem).
send "set distsql_workmem='32GiB';\r"
eexpect SET
send "select a, random() AS r, repeat('a', 1000) from generate_series(1,10000000) g(a) order by r;\r"
eexpect "connection lost"

# Check that the query crashed the server
set spawn_id $shell_spawn_id
# Error is either an explicit "out of memory" emitted by Go or one of the errors
# thrown by CGo.
#
# "fatal error: unexpected signal during runtime execution" and "segmentation
# violation" might seem like they don't belong here, but it's an artifact of how
# we're limiting the memory usage of the CRDB process. In particular, ulimit is
# a "user limit" which is enforced not at the OS kernel level (i.e. not via
# oomkiller) but at the process execution level. As a result, memory allocation
# error doesn't kill the process, so it keeps on running and can hit this "fatal
# error" or "segmentation violation" later on.
expect {
    "out of memory" {}
    "cannot allocate memory" {}
    "std::bad_alloc" {}
    "Resource temporarily unavailable" {}
    "_Cfunc_calloc" {}
    "fatal error: unexpected signal during runtime execution" {}
    "segmentation violation" {}
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
# Re-launch the server with relatively low limit for SQL memory. A couple of
# notable callouts:
# - we couldn't have created a new cluster from scratch with such a low SQL
#   memory limit because migrations wouldn't be able to run. However, we're
#   reusing existing data directory, so we're able to start up the node.
# - the default value of distsql_workmem (64MiB) is used and is larger than the
#   SQL memory limit. As a result, we don't attempt to spill to disk and hit the
#   root memory limit.
set spawn_id $shell_spawn_id
send "$argv start-single-node --insecure --max-sql-memory=1000K -s=path=logs/db \r"
eexpect "restarted pre-existing node"
sleep 2

# Now try the large query again.
set spawn_id $client_spawn_id
send "select 1;\r"
eexpect root@
send "select a, random() AS r, repeat('a', 1000) from generate_series(1,10000000) g(a) order by r;\r"
eexpect "root: memory budget exceeded"
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
