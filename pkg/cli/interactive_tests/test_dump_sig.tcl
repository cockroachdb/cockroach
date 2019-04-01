#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash

send "PS1='\\h:''/# '\r"
eexpect ":/# "

start_test "Check that the server emits a goroutine dump upon receiving signal"
send "$argv start --insecure --pid-file=server_pid --log-dir=logs --logtostderr\r"
eexpect "CockroachDB node starting"

system "kill -QUIT `cat server_pid`"
eexpect "\nE*stack trace dump requested by signal:"
eexpect "log/clog.go"
eexpect "quit received, flushing logs"
end_test

# stop the interactive server
system "$argv quit --insecure"
eexpect ":/# "

start_test "Check that the server is still running after signal."
# Start server in background.
start_server $argv
# Start client.
send "$argv sql --insecure\r"
eexpect root@
# Dump goroutines in server.
system "kill -QUIT `cat server_pid`"
# Expect client to still work.
send "SELECT 1 AS FOO;\r"
eexpect "foo"
eexpect "1 row"
# stop the client.
interrupt
eexpect ":/# "
# Stop the server.
stop_server $argv
end_test

start_test "Check that the client also can generate goroutine dumps."
send "$argv demo\r"
eexpect root@
# Dump goroutines in server.
system "killall -QUIT `basename \$(realpath $argv)`"
eexpect "E*stack trace dump requested by signal:"
eexpect "log/clog.go"
end_test

start_test "Check that the client still works after SIGQUIT."
send "SELECT 1 AS FOO;\r"
eexpect "foo"
eexpect "1 row"
end_test

interrupt
eexpect ":/# "
send "exit\r"
eexpect eof
