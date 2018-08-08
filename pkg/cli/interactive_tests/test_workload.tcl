#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that the tpcc workload is registered."
send "$argv workload run --help\r"
eexpect "tpcc"
end_test

start_test "Sanity check workload fixtures url."
send "$argv workload fixtures url tpcc\r"
eexpect "gs://cockroach-fixtures/workload/tpcc"
end_test

# Clean up.
send "exit 0\r"
eexpect eof
