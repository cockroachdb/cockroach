#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Make a server with a tiny log buffer so as to force frequent log rotation.
system "mkfifo pid_fifo || true; $argv start --insecure --pid-file=pid_fifo --background -s=path=logs/db --log-file-max-size=2k >>logs/expect-cmd.log 2>&1 & cat pid_fifo > server_pid"

start_test "Check that the cluster ID is reported at the start of new log files."
system "grep '\\\[config\\\] clusterID:' logs/db/logs/cockroach.log"
end_test

stop_server $argv
