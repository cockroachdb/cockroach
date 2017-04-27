#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# flaky skip: #15391
exit 0

spawn /bin/bash
send "PS1='\\h:''/# '\r"
eexpect ":/# "

start_test "Check that the server shuts down upon receiving SIGTERM"
send "$argv start --insecure --pid-file=server_pid\r"
eexpect "initialized"

system "kill `cat server_pid`"
eexpect "initiating graceful shutdown"
eexpect "shutdown completed"
eexpect ":/# "
end_test

start_test "Check that server stopped with SIGTERM finishes with exit code 0. (#9051)"
send "echo \$?\r"
eexpect "0\r\n"
eexpect ":/# "
end_test

start_test "Check that the server shuts down upon receiving Ctrl+C."
send "$argv start --insecure --pid-file=server_pid\r"
eexpect "restarted"

interrupt
eexpect "initiating graceful shutdown"
eexpect "shutdown completed"
eexpect ":/# "
end_test

start_test "Check that Ctrl+C finishes with exit code 1. (#9051)"
send "echo \$?\r"
eexpect "1\r\n"
eexpect ":/# "
end_test

start_test "Check that the server shuts down fast upon receiving Ctrl+C twice."
send "$argv start --insecure --pid-file=server_pid\r"
eexpect "restarted"
interrupt
eexpect "initiating graceful shutdown"
interrupt
# The server could finish draining before the second interrupt is sent.
expect {
    "hard shutdown" {}
    "shutdown completed" {}
    timeout { handle_timeout "server shutdown message" }
}
eexpect ":/# "
end_test

start_test "Check that Ctrl+C twice finishes with exit code 130. (#9051)"
send "echo \$?\r"
eexpect "130\r\n"
eexpect ":/# "
end_test

send "exit\r"
eexpect eof

stop_server $argv
