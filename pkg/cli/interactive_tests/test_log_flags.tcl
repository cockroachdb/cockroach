#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

# Perform command-line checking for logging flags. We cannot use a
# regular unit test for this, because the logging flags are declared
# for the global `CommandLine` object of package `flag`, and any
# errors when parsing flags in that context cause the (test) process
# to exit entirely (it has errorHandling set to ExitOnError).

start_test "Check that log files are created by default in the store directory."
send "$argv start-single-node --insecure --store=path=logs/mystore\r"
eexpect "node starting"
interrupt
eexpect ":/# "
send "ls logs/mystore/logs\r"
eexpect "cockroach.log"
eexpect ":/# "
end_test

start_test "Check that an empty -log-dir disables file logging."
send "$argv start-single-node --insecure --store=path=logs/mystore2 --log-dir=\r"
eexpect "node starting"
interrupt
eexpect ":/# "
send "ls logs/mystore2/logs 2>/dev/null | grep -vE 'heap_profiler|goroutine_dump' | wc -l\r"
eexpect "0"
eexpect ":/# "
end_test

start_test "Check that leading tildes are properly rejected."
send "$argv start-single-node --insecure -s=path=logs/db --log-dir=\~/blah\r"
eexpect "log directory cannot start with '~'"
eexpect ":/# "
end_test

start_test "Check that the user can override."
send "$argv start-single-node --insecure -s=path=logs/db --log-dir=logs/blah/\~/blah\r"
eexpect "logs: *blah/~/blah"
interrupt
eexpect ":/# "
end_test

start_test "Check that TRUE and FALSE are valid values for the severity flags."
send "$argv start-single-node --insecure -s=path=logs/db --logtostderr=false\r"
eexpect "node starting"
interrupt
eexpect ":/# "
send "$argv start-single-node --insecure -s=path=logs/db --logtostderr=true\r"
eexpect "node starting"
interrupt
eexpect ":/# "
send "$argv start-single-node --insecure -s=path=logs/db --logtostderr=2\r"
eexpect "node starting"
interrupt
eexpect ":/# "
send "$argv start-single-node --insecure -s=path=logs/db --logtostderr=cantparse\r"
eexpect "parsing \"cantparse\": invalid syntax"
eexpect ":/# "
end_test

start_test "Check that conflicting legacy and new flags are properly rejected for server commands"
send "$argv start-single-node --insecure --logtostderr=true --log=abc\r"
eexpect "log is incompatible with legacy discrete logging flag"
eexpect ":/# "
end_test

start_test "Check that conflicting legacy and new flags are properly rejected for client commands"
send "$argv sql --insecure --logtostderr=true --log=abc\r"
eexpect "log is incompatible with legacy discrete logging flag"
eexpect ":/# "
end_test

start_test "Check that the log flag is properly recognized for non-server commands"
send "$argv debug reset-quorum 123 --log='sinks: {stderr: {format: json }}'\r"
eexpect "\"severity\":\"ERROR\""
eexpect "connection to server failed"
eexpect ":/# "

send "exit 0\r"
eexpect eof
