#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

send "$argv sql\r"
eexpect root@

start_test "Check that an error in the last statement is propagated to the shell."
send "select ++;\r"
eexpect "syntax error"
eexpect root@
send "\\q\r"
eexpect ":/# "
send "echo hello \$?\r"
eexpect "hello 1"
eexpect ":/# "
end_test

start_test "Check that an incomplete last statement in interactive mode is not executed."
send "$argv sql\r"
eexpect root@
send "drop database if exists t cascade; create database t; create table t.foo(x int);\r"
eexpect "CREATE TABLE"
eexpect root@
send "insert into t.foo(x) values (42)\r"
eexpect " ->"
send_eof
eexpect ":/# "

send "$argv sql\r"
eexpect root@
send "select count(*) from t.foo;\r"
eexpect "0"
eexpect root@
send "\\q\r"
eexpect ":/# "
end_test

start_test "Check that an incomplete last statement in non-interactive mode is not executed, and fails with a warning. #8838"
send "echo 'insert into t.foo(x) values (42)' | $argv sql\r"
eexpect "missing semicolon at end of statement"
eexpect ":/# "
end_test

start_test "Check that a final comment after a missing semicolon and without newline is properly ignored. #9482"
send "echo 'insert into t.foo(x) values (42)--;' | $argv sql\r"
eexpect "missing semicolon at end of statement"
eexpect ":/# "

send "echo 'select count(*) from t.foo;' | $argv sql\r"
eexpect "0"
eexpect ":/# "
end_test

start_test "Check that a complete last statement terminated with a semicolon just before EOF and without a newline is properly executed. #7328"
send "printf 'insert into t.foo(x) values(42);' | $argv sql\r"
eexpect "INSERT"
eexpect ":/# "
send "echo 'select count(*) from t.foo;' | $argv sql\r"
eexpect "1"
eexpect ":/# "
end_test

start_test "Check that a final comment after a final statement does not cause an error message. #9482"
send "printf 'select 1 as woo;-- final comment' | $argv sql | cat\r"
eexpect "woo\r\n1\r\n:/# "
end_test

start_test "Check that a final comment does not cause an error message. #9243"
send "printf 'select 1 as woo;\\n-- final comment' | $argv sql | cat\r"
eexpect "woo\r\n1\r\n:/# "
end_test

# Finally terminate with Ctrl+C
send "exit\r"
eexpect eof

stop_server $argv
