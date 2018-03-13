#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set logfile logs/db/logs/cockroach.log

start_server $argv

start_test "Check that SIGUSR1 says it flushes the log"
flush_server_logs
system "grep 'signal.*received, flushing logs' $logfile"
end_test

start_test "Check that SIGUSR2 flushes and rotates the logs"
rotate_server_logs
# The log have been rotated so the flush/rotate messages should not be there any more.
system "if grep 'signal.*received, flushing logs' $logfile; then false; fi"
system "if grep 'signal.*received, rotating logs' $logfile; then false; fi"
# However they must be in the directory in some previous log file.
system "grep 'signal.*received, flushing logs' logs/db/logs/*.log"
system "grep 'signal.*received, rotating logs' logs/db/logs/*.log"
end_test

stop_server $argv
