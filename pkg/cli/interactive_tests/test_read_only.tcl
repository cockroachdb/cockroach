#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

send "$argv sql -e 'CREATE TABLE foo (i INT PRIMARY KEY);'\r"
eexpect "CREATE TABLE"
eexpect ":/# "

start_test "Check that the read-only flag works non-interactively"
send "$argv sql --read-only -e 'delete from foo where i = 1'\r"
eexpect "ERROR: cannot execute DELETE in a read-only transaction"
eexpect ":/# "
end_test

start_test "Check that the read-only flag works interactively"
send "$argv sql --no-line-editor --read-only | cat\r"
# We can't immediately send input, because the shell will eat stdin just before it's ready.
eexpect "brief introduction"
sleep 0.4
send "show default_transaction_read_only;\r"
eexpect "on"
eexpect "\r\n"
send "delete from foo where i = 0;\r"
eexpect "ERROR: cannot execute DELETE in a read-only transaction"
send "\\q\r"
eexpect ":/# "
end_test

stop_server $argv
