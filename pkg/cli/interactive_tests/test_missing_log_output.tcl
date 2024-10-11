#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that a server encountering a fatal error when not logging to stderr exits with the right code."
send "$argv start-single-node -s=path=logs/db --insecure\r"
eexpect "CockroachDB node starting"
system "$argv sql --insecure -e \"select crdb_internal.force_log_fatal('helloworld')\" || true"
eexpect ":/# "
send "echo \$?\r"
eexpect "7"
eexpect ":/# "
end_test

start_test "Check that a broken stderr prints a message to the log files."
# We use --log-file-max-size to avoid rotations during the test.
send "$argv start-single-node -s=path=logs/db --log-file-max-size=100M --insecure --logtostderr --vmodule=*=1 2>&1 | cat\r"
eexpect "CockroachDB node starting"
system "killall cat"
eexpect ":/# "
# NB: we can't just grep for the broken pipe output, because it may take
# a while for the server to initiate the next log line where it will detect
# the broken pipe error.
#
# We use -F and not -f because the log file may be rotated during the test.
#
# We also watch all the log files in the directory, because the error
# is only reported on the logger which is writing first after stderr
# is has been broken, and that may be the secondary logger.
send "tail -F `find logs/db/logs -type l`\r"
eexpect "error: write */dev/stderr*: broken pipe"
interrupt
eexpect ":/# "
end_test

start_test "Check that a broken log file prints a message to stderr."
# The path that we pass to the --log-dir will already exist as a file.
system "mkdir -p logs"
system "touch logs/broken"
send "$argv start-single-node -s=path=logs/db --log-dir=logs/broken --insecure --logtostderr\r"
eexpect "unable to create log directory"
eexpect ":/# "
end_test

start_test "Check that a server started with only in-memory stores and no --log-dir automatically logs to stderr."
send "$argv start-single-node --insecure --store=type=mem,size=1GiB\r"
eexpect "CockroachDB node starting"
end_test

# Stop it.
interrupt
eexpect ":/# "

# Disable replication so as to avoid spurious purgatory errors.
start_server $argv
send "$argv sql --insecure -e \"ALTER RANGE default CONFIGURE ZONE USING num_replicas = 1\"\r"
eexpect {CONFIGURE ZONE [0-9]}
eexpect ":/# "
stop_server $argv

start_test "Check that a server started with --logtostderr logs even info messages to stderr."
send "$argv start-single-node --max-sql-memory=128MB -s=path=logs/db --insecure --logtostderr\r"
eexpect "CockroachDB node starting"
end_test

# Stop it.
interrupt
interrupt
eexpect ":/# "
system "rm -rf logs/db"

start_test "Check that --logtostderr can override the threshold but no error is printed on startup"
send "echo marker; $argv start-single-node --max-sql-memory=128MB -s=path=logs/db --insecure --logtostderr=ERROR 2>&1 | grep -v '^\\*'\r"
eexpect "marker\r\nCockroachDB node starting"
end_test

# Stop it.
interrupt
interrupt
eexpect ":/# "
system "rm -rf logs/db"


start_test "Check that panic reports are printed to the log even when --logtostderr is specified"
send "$argv start-single-node --max-sql-memory=128MB -s=path=logs/db --insecure --logtostderr\r"
eexpect "CockroachDB node starting"

system "($argv sql --insecure -e \"select crdb_internal.force_panic('helloworld')\" || true)&"
# Check the panic is reported on the server's stderr
eexpect "a SQL panic has occurred"
eexpect "panic: *helloworld"
eexpect "stack trace"
eexpect ":/# "
# Check the panic is reported on the server log file
send "cat logs/db/logs/cockroach.log\r"
eexpect "a SQL panic has occurred"
eexpect "helloworld"
eexpect "a panic has occurred"
eexpect ":/# "
send "cat logs/db/logs/cockroach-stderr.log\r"
eexpect "panic"
eexpect "helloworld"
eexpect "goroutine"
eexpect ":/# "

end_test

start_server $argv

start_test "Test that init does not show INFO by default with --logtostderr"
# Test init as non-start command, this time with --logtostderr. Test
# that the default logging level is WARNING, so that no INFO messages
# are printed between the marker and the (first line) error message
# from init. Init will error out because the server is already initialized.
send "echo marker; $argv init --logtostderr 2>&1\r"
eexpect "marker\r\nERROR: cluster has already been initialized\r\nFailed running \"init\""
eexpect ":/# "
end_test

start_test "Check that `--logtostderr` can override the default"
send "$argv init --logtostderr=INFO --vmodule=peer=1\r"
eexpect "probe starting"
eexpect ":/# "
end_test

stop_server $argv

send "exit\r"
eexpect eof
