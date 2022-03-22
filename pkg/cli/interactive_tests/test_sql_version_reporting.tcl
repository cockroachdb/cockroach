#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql

start_test "Check that the client starts with the welcome message."
eexpect "# Welcome to the CockroachDB SQL shell."
end_test

start_test "Check that the client reports the server version, and correctly detects the version is the same as the client."
eexpect "# Server version: CockroachDB"
eexpect "(same version as client)"
end_test

start_test "Check that cluster ID is reported as well."
eexpect "# Cluster ID: "
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
eexpect "connection lost"
eexpect "opening new connection: all session settings will be lost"
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
eexpect "error"
eexpect "the cluster ID has changed!"
eexpect "Previous ID:"
eexpect "New ID:"
eexpect root@
end_test

send_eof
eexpect eof

stop_server $argv

start_test "Check that the client picks up a new server version, and warns about lower versions."
set env(COCKROACH_TESTING_VERSION_TAG) "v0.1.0-fakever"
start_server $argv
set env(COCKROACH_TESTING_VERSION_TAG) "v0.2.0-fakever"
spawn $argv sql
send "select 1;\r"
eexpect "# Client version: CockroachDB"
eexpect "# Server version: CockroachDB"
eexpect "fakever"
eexpect "warning: server version older than client"
eexpect root@
end_test

send_eof
eexpect eof

stop_server $argv
