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

start_test "Check that the zip command reports when large files are being transferred"
# Create a fake large file.
system "dd if=/dev/urandom of=logs/db/logs/heap_profiler/memprof.2021-04-26T14_19_50.909.26469048.pprof bs=1048576 count=15"
# Retrieve all files, including a larger period of time so that the fake
# file gets included.
send "$argv debug zip --cpu-profile-duration=0 --files-from=2000-01-01 /dev/null >/dev/null\r"
# Expect the warning and hint.
eexpect "warning: output file size exceeds"
eexpect "hint"
eexpect "refine what data gets included"
eexpect ":/# "
end_test


stop_server $argv
