#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set logfile logs/db/logs/cockroach.log

start_server $argv

start_test "Check that SIGHUP says it flushes the log"
flush_server_logs
system "grep 'signal.*received, flushing logs' $logfile"
end_test

stop_server $argv
