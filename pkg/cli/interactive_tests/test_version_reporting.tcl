#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql

start_test "Check that the client starts with the welcome message."
eexpect "# Welcome to the cockroach SQL interface."
end_test

start_test "Check that the client reports the server version, and correctly detects the version is the same as the client."
eexpect "# Server version: CockroachDB"
eexpect "(same version as client)"
end_test

start_test "Check that cluster ID and node ID are reported as well."
eexpect "# Cluster ID: "
eexpect "# Node ID: 1"
end_test

start_test "Check that the help part of the introductory message is at the end."
eexpect "for a brief introduction"
end_test

eexpect root@

start_test "Check that a reconnect without version change is quiet."
# We need to force since the open connection may prevent a quick
# graceful shutdown.
force_stop_server $argv
start_server $argv
send "select 1;\r"
eexpect "driver: bad connection"
# Check that the prompt immediately succeeds the error message
eexpect "connection lost; opening new connection: all session settings will be lost"
expect {
    "\r\n# " {
	report "unexpected server message"
	exit 1
    }
    "root@" {}
}

# Check the reconnect did succeed - this also resets the connection state to good.
send "select 1;\r"
eexpect "1 row"
eexpect root@
end_test

start_test "Check that the client picks up a new cluster ID."
force_stop_server $argv
system "mv logs/db logs/db-bak"
start_server $argv
send "select 1;\r"
eexpect "opening new connection"
eexpect "# WARNING: the cluster ID has changed!"
eexpect "# Previous cluster ID:"
eexpect "# Cluster ID"
eexpect root@
end_test

start_test "Check that the client picks up a new server version."
force_stop_server $argv
set env(COCKROACH_TESTING_VERSION_TAG) "fakever"
start_server $argv
send "select 1;\r"
eexpect "opening new connection"
eexpect "# Client version: CockroachDB"
eexpect "# Server version: CockroachDB"
eexpect "fakever"
eexpect root@
end_test

start_test "Check that the client picks up a new node ID."
force_stop_server $argv
# restart node 1 on a different port
system "$argv start --insecure --pid-file=pid_fifo --background -s=path=logs/db --http-port=8081 --port=26255 2>&1 & cat pid_fifo >server_pid2"
# start node 2 on original port
system "$argv start --insecure --pid-file=pid_fifo --background -s=path=logs/db2 --join=\${PGHOST-localhost}:26255 2>&1 & cat pid_fifo >server_pid"

send "select 1;\r"
eexpect "opening new connection"
eexpect "# Node ID: 2"
eexpect root@
end_test

interrupt
eexpect eof

# Kill the servers. Don't be gentle, otherwise we're timing out always.
system "kill -9 `cat server_pid` `cat server_pid2`"
