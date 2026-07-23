#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_server $argv

# A server start populates a log directory with multiple files. We expect
# at least cockroach-stderr.*, cockroach.* and cockroach-pebble.*.

start_test "Check that --include-files excludes files that do not match the pattern"
send "$argv debug zip --cpu-profile-duration=0 --include-files='cockroach.*' /dev/null\r"

eexpect "log files found"
eexpect "skipping excluded log file: cockroach-stderr."
eexpect "skipping excluded log file: cockroach-pebble."
eexpect "\[log file: cockroach."
eexpect ":/# "
end_test

start_test "Check that --excludes-files excludes files that match the pattern"
send "$argv debug zip --cpu-profile-duration=0 --exclude-files='cockroach.*' /dev/null\r"

eexpect "log files found"
eexpect "\[log file: cockroach-stderr."
eexpect "\[log file: cockroach-pebble."
eexpect "skipping excluded log file: cockroach."
eexpect ":/# "
end_test


stop_server $argv
