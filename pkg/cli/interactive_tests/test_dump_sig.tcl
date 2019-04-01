#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash

send "PS1='\\h:''/# '\r"
eexpect ":/# "

start_test "Check that the server emits a goroutine dump upon receiving signal"
send "$argv start --insecure --pid-file=server_pid --log-dir=logs --logtostderr\r"
eexpect "CockroachDB node starting"

system "kill -QUIT `cat server_pid`"
eexpect "received signal 'quit'"
eexpect "\nI*stack traces:"
eexpect "RunAsyncTask"
eexpect "server drained and shutdown completed"
# Check that the server eventually terminates.
eexpect ":/# "
end_test

start_test "Check that the client also can generate goroutine dumps."
send "$argv demo --empty\r"
eexpect root@
# Dump goroutines in server.
system "killall -QUIT `basename \$(realpath $argv)`"
eexpect "SIGQUIT: quit"
eexpect "RunAsyncTask"
# Check that the client terminates.
eexpect ":/# "
end_test

send "exit\r"
eexpect eof
