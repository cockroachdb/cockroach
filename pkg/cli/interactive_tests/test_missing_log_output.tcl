#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that a server encountering a fatal error when not logging to stderr shows the fatal error."
send "$argv start -s=path=logs/db --insecure\r"
eexpect "CockroachDB node starting"
system "$argv sql --insecure -e \"select crdb_internal.force_log_fatal('helloworld')\" || true"
eexpect "\r\nF"
eexpect "helloworld"
eexpect ":/# "
send "echo \$?\r"
eexpect "255"
eexpect ":/# "
end_test

start_test "Check that a broken stderr prints a message to the log files."
send "$argv start -s=path=logs/db --insecure --logtostderr --vmodule=*=1 2>&1 | cat\r"
eexpect "CockroachDB node starting"
system "killall cat"
eexpect ":/# "
system "grep -F 'log: exiting because of error: write /dev/stderr: broken pipe' logs/db/logs/cockroach.log"
end_test

start_test "Check that a broken log file prints a message to stderr."
# The path that we pass to the --log-dir will already exist as a file.
system "mkdir -p logs"
system "touch logs/broken"
send "$argv start -s=path=logs/db --log-dir=logs/broken --insecure --logtostderr\r"
eexpect "unable to create log directory"
eexpect ":/# "
end_test

start_test "Check that a server started with only in-memory stores and no --log-dir automatically logs to stderr."
send "$argv start --insecure --store=type=mem,size=1GiB\r"
eexpect "CockroachDB node starting"
end_test

# Stop it.
interrupt
eexpect ":/# "

# Disable replication so as to avoid spurious purgatory errors.
start_server $argv
send "$argv sql --insecure -e \"ALTER RANGE default CONFIGURE ZONE USING num_replicas = 1\"\r"
eexpect "CONFIGURE ZONE 1"
eexpect ":/# "
stop_server $argv

start_test "Check that a server started with --logtostderr logs even info messages to stderr."
send "$argv start -s=path=logs/db --insecure --logtostderr\r"
eexpect "CockroachDB node starting"
end_test

# Stop it.
interrupt
eexpect ":/# "

start_test "Check that --logtostderr can override the threshold but no error is printed on startup"
send "echo marker; $argv start -s=path=logs/db --insecure --logtostderr=ERROR 2>&1 | grep -v '^\\*'\r"
eexpect "marker\r\nCockroachDB node starting"
end_test

# Stop it.
interrupt
eexpect ":/# "

start_test "Check that panic reports are printed to the log even when --logtostderr is specified"
send "$argv start -s=path=logs/db --insecure --logtostderr\r"
eexpect "CockroachDB node starting"

system "$argv sql --insecure -e \"select crdb_internal.force_panic('helloworld')\" || true"
# Check the panic is reported on the server's stderr
eexpect "panic: helloworld"
eexpect "panic while executing"
eexpect "goroutine"
eexpect ":/# "
# Check the panic is reported on the server log file
send "cat logs/db/logs/cockroach.log\r"
eexpect "a SQL panic has occurred"
eexpect "helloworld"
eexpect "a panic has occurred"
eexpect "panic while executing"
eexpect "goroutine"
eexpect ":/# "

end_test



start_server $argv

start_test "Test that quit does not show INFO by default with --logtostderr"
# Test quit as non-start command, this time with --logtostderr. Test
# that the default logging level is WARNING, so that no INFO messages
# are printed between the marker and the (first line) error message
# from quit. Quit will error out because the server is already stopped.
send "echo marker; $argv quit --logtostderr 2>&1 | grep -vE '^\[WEF\]\[0-9\]+'\r"
eexpect "marker\r\nok"
eexpect ":/# "
end_test

start_test "Check that `--logtostderr` can override the default"
send "$argv quit --logtostderr=INFO --vmodule=stopper=1\r"
eexpect "stop has been called"
eexpect ":/# "
end_test

send "exit\r"
eexpect eof
