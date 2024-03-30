#! /usr/bin/env expect -f
#
source [file join [file dirnam $argv0] common.tcl]

# Start a server with a --join flag so the init command is required
# (even though we have no intention of starting a second node).  Note
# that unlike other expect-based tests, this one doesn't use a fifo
# for --pid_file because we don't want reads from that fifo to change
# the outcome.
system "$argv start --insecure --pid-file=server_pid -s=path=logs/db --listen-addr=localhost --background --join=localhost:26258 >>logs/expect-cmd.log 2>&1"

start_test "Check that the server has informed us and the log file that it was ready before forking off in the background"
system "grep -q 'initial startup completed' logs/db/logs/cockroach.log"
system "grep -q 'will now attempt to join a running cluster, or wait' logs/db/logs/cockroach.log"
end_test

start_test "Check that init --virtualized creates a main virtual cluster"

system "$argv init --insecure --host=localhost --virtualized"

# Start a shell and expect that we end up inside a secondary virtual
# cluster.
spawn $argv sql --no-line-editor
send "SELECT crdb_internal.pretty_key(crdb_internal.table_span(1)\[1\], 0);\r"
eexpect "/3/Table/1"

stop_server $argv
end_test
