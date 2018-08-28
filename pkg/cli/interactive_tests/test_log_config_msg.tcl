#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# We stop the server before each test to ensure the log file is
# flushed and not in the middle of a rotation.

start_server $argv
stop_server $argv

start_test "Check that the cluster ID is reported at the start of the first log file."
system "grep -qF '\[config\] clusterID:' logs/db/logs/cockroach.log"
end_test


# Make a server with a tiny log buffer so as to force frequent log rotation.
system "mkfifo url_fifo || true; $argv start --insecure --pid-file=server_pid --listening-url-file=url_fifo --background -s=path=logs/db --log-file-max-size=2k >>logs/expect-cmd.log 2>&1 & cat url_fifo > server_url"
stop_server $argv

start_test "Check that the cluster ID is reported at the start of new log files."
# Verify that the string "restarted pre-existing node" can be found
# somewhere. This ensures that if this string ever changes, the test
# below won't report a false negative.
system "grep -q 'restarted pre-existing node' logs/db/logs/*.log"
# Verify that "cockroach.log" is not the file where the server reports
# it just started.
system "if grep -q 'restarted pre-existing node' logs/db/logs/cockroach.log; then false; fi"
# Verify that the last log file does contain the cluster ID.
system "grep -qF '\[config\] clusterID:' logs/db/logs/cockroach.log"
end_test
