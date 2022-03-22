#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash

send "PS1='\\h:''/# '\r"
eexpect ":/# "

start_test "Check that the server emits a goroutine dump upon receiving signal"
send "$argv start-single-node --insecure --pid-file=server_pid --store=path=logs/db --logtostderr\r"
eexpect "CockroachDB node starting"

system "kill -QUIT `cat server_pid`"
eexpect "\nI*stack traces:"
eexpect "RunAsyncTask"

system "kill -QUIT `cat server_pid`"
eexpect "\nI*stack traces:"
eexpect "RunAsyncTask"

interrupt
# Check that the server eventually terminates.
eexpect ":/# "
end_test

start_test "Check that the client also can generate goroutine dumps."
send "$argv demo --no-example-database\r"
eexpect root@
# Dump goroutines in server.
system "killall -QUIT `basename \$(realpath $argv)`"
eexpect "SIGQUIT received"
eexpect "RunAsyncTask"

# Check that the client has survived.
send "\r"
eexpect root@

# Finish the test.
send_eof
eexpect ":/# "
end_test

send "exit\r"
eexpect eof
