#! /usr/bin/env expect -f
#
source [file join [file dirnam $argv0] common.tcl]

# Start a server with a --join flag so the init command is required
# (even though we have no intention of starting a second node).  Note that unlike other
# expect-based tests, this one doesn't use a fifo for --pid_file
# because we don't want reads from that fifo to change the outcome.
system "$argv start --insecure --pid-file=server_pid -s=path=logs/db --listen-addr=localhost --background --join=localhost:26258 --cluster-name=foo >>logs/expect-cmd.log 2>&1"

start_test "Check that the server has informed us and the log file that it was ready before forking off in the background"
system "grep -q 'initial startup completed' logs/db/logs/cockroach.log"
system "grep -q 'will now attempt to join a running cluster, or wait' logs/db/logs/cockroach.log"
end_test

start_test "Check that init works with a cluster-name provided"
system "$argv init --insecure --host=localhost --cluster-name=foo"
end_test

start_test "Check that decommission works with a cluster-name provided"
send "$argv node decommission 1 --insecure --host=localhost --wait=none --cluster-name=foo\r"
end_test

start_test "Check that recommission works with cluster name verification disabled"
send "$argv node recommission 1 --insecure --host=localhost --disable-cluster-name-verification\r"
end_test

start_test "Check that debug commands work with a cluster name provided"
send "$argv debug gossip-values --insecure --host=localhost --cluster-name=foo\r"
end_test

start_test "Check that quit works with a cluster-name provided"
send "$argv quit --insecure --host=localhost --wait=none --cluster-name=foo\r"
end_test

stop_server $argv
