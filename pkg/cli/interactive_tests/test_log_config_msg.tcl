#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# We stop the server before each test to ensure the log file is
# flushed and not in the middle of a rotation.

start_server $argv

start_test "Check that the cluster and node ID is reported at the start of the first log file."
spawn tail -n 1000 -F logs/db/logs/cockroach.log
eexpect "node startup completed"
eexpect "start.go*ClusterID:"
eexpect "start.go*nodeID:"
end_test

stop_server $argv


# Restart the server, to check that the server identifiers are also reported after restarts.
start_server $argv
stop_server $argv

start_test "Check that the cluster and node ID is reported at the start of new log files."
# Verify that the string "restarted pre-existing node" can be found
# somewhere.
system "grep -q 'restarted pre-existing node' logs/db/logs/*.log"
# Verify that the last log file does contain the cluster ID.
system "grep -q 'start\.go.*ClusterID:' logs/db/logs/cockroach.log"
system "grep -q 'start\.go.*nodeID:' logs/db/logs/cockroach.log"
end_test
