#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that dangerous statements are properly rejected when running interactively with terminal output."
send "$argv sql\r"
eexpect root@
send "create database d;\rcreate table d.t(x int);\r"
eexpect "CREATE"
eexpect "CREATE"
eexpect root@

send "delete from d.t;\r"
eexpect "rejected (sql_safe_updates = true): DELETE without WHERE clause"
eexpect root@
end_test

send "\\q\r"
eexpect ":/# "

start_test "Check that dangerous statements are properly rejected when inputting from user even with output redirected."
send "$argv sql | cat\r"
# We can't immediately send input, because the shell will eat stdin just before it's ready.
eexpect "brief introduction"
sleep 0.4
send "show sql_safe_updates;\r"
eexpect "on"
eexpect "\r\n"
send "delete from d.t;\r"
eexpect "rejected (sql_safe_updates = true): DELETE without WHERE clause"
send "\\q\r"
eexpect ":/# "
end_test

start_test "Check that dangerous statements are not rejected when input redirected."
send "echo 'delete from d.t;' | $argv sql \r"
eexpect "DELETE"
eexpect ":/# "
end_test

start_test "Check that dangerous statements are not rejected when using -e."
send "$argv sql -e 'delete from d.t'\r"
eexpect "DELETE"
eexpect ":/# "
end_test

start_test "Check that dangerous statements are properly rejected when using --safe-updates -e."
send "$argv sql --safe-updates -e 'delete from d.t'\r"
eexpect "rejected (sql_safe_updates = true): DELETE without WHERE clause"
eexpect ":/# "
end_test

stop_server $argv
