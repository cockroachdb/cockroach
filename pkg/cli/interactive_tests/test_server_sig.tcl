#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
set shell_spawn_id $spawn_id

send "PS1='\\h:''/# '\r"
eexpect ":/# "

start_test "Check that the server shuts down upon receiving SIGTERM"
send "$argv start-single-node --insecure --pid-file=server_pid --log-dir=logs -s=path=logs/db \r"
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
send "$argv start-single-node --insecure --pid-file=server_pid --log-dir=logs -s=path=logs/db \r"
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

# Start a server via the shell
send "$argv start-single-node --insecure --pid-file=server_pid --log-dir=logs -s=path=logs/db \r"
eexpect "restarted"

# Make a client open a connection and keep using it with an open txn.
spawn $argv sql
set client_spawn_id $spawn_id
eexpect root@
send "begin;\r\rselect 1;\r"
eexpect "1 row"
eexpect root@

# Now interrupt the server twice.
set spawn_id $shell_spawn_id
send "\003"
eexpect "graceful shutdown"
send "\003"
# There's still a very small chance the server could finish draining
# before the second interrupt is sent, but oh well.
set interrupted false
expect {
    "hard shutdown" {
        global interrupted
        set interrupted true
    }
    "shutdown completed" {}
    timeout { handle_timeout "server shutdown message" }
}
eexpect ":/# "
end_test

if { $interrupted == true } {
  start_test "Check that Ctrl+C twice finishes with exit code 130. (#9051)"
  send "echo \$?\r"
  eexpect "130\r\n"
  eexpect ":/# "
  end_test
}

# terminate the client cleanly
set spawn_id $client_spawn_id
send "\\q\r"
eexpect eof

start_test "Check that the server shuts keeps gracefully shutting down upon receiving SIGTERM twice."

# Start a server via the shell
set spawn_id $shell_spawn_id
send "$argv start-single-node --insecure --pid-file=server_pid --log-dir=logs -s=path=logs/db \r"
eexpect "restarted"

# Make a client open a connection and keep using it with an open txn.
spawn $argv sql
set client_spawn_id $spawn_id
eexpect root@
send "begin;\r\rselect 1;\r"
eexpect "1 row"
eexpect root@

# Now interrupt the server twice.
set spawn_id $shell_spawn_id
system "kill `cat server_pid`"
eexpect "graceful shutdown"
system "kill `cat server_pid`"
# There's still a very small chance the server could finish draining
# before the second signal is sent, but oh well.
expect {
    "shutdown completed" {}
    "hard shutdown" {
        report "unexpected hard shutdown"
        exit 1
    }
    timeout { handle_timeout "server shutdown message" }
}
eexpect ":/# "
end_test

# Terminate the SQL shell.
set spawn_id $shell_spawn_id
send "exit\r"
eexpect eof
