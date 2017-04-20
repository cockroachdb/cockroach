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

# Check that log files are created by default in the store directory.
send "$argv start --insecure --store=path=mystore\r"
eexpect "node starting"
send "\003"
eexpect ":/# "
send "ls mystore/logs\r"
eexpect "cockroach.log"
eexpect ":/# "

# Check that an empty `-log-dir` disables file logging.
send "$argv start --insecure --store=path=mystore2 --log-dir=\r"
eexpect "node starting"
send "\003"
eexpect ":/# "
send "ls mystore2/logs 2>/dev/null | wc -l\r"
eexpect "0"
eexpect ":/# "

# Check that leading tildes are properly rejected.
send "$argv start --insecure --log-dir=\~/blah\r"
eexpect "log directory cannot start with '~'"
eexpect ":/# "

# Check that the user can override.
send "$argv start --insecure --log-dir=blah/\~/blah\r"
eexpect "logs: *blah/~/blah"
send "\003"
eexpect ":/# "

# Check that TRUE and FALSE are valid values for the severity flags.
send "$argv start --insecure --logtostderr=false\r"
eexpect "node starting"
send "\003"
eexpect ":/# "
send "$argv start --insecure --logtostderr=true\r"
eexpect "node starting"
send "\003"
eexpect ":/# "
send "$argv start --insecure --logtostderr=2\r"
eexpect "node starting"
send "\003"
eexpect ":/# "
send "$argv start --insecure --logtostderr=cantparse\r"
eexpect "parsing \"cantparse\": invalid syntax"
eexpect ":/# "

send "exit 0\r"
eexpect eof
