#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

send "$argv sql\r"
eexpect root@

# Check that an error in the last statement is propagated to the shell.
send "select ++;\r"
eexpect "syntax error"
eexpect root@
send "\\q\r"
eexpect ":/# "
send "echo hello \$?\r"
eexpect "hello 1"
eexpect ":/# "

# Check that an incomplete last statement in interactive mode is not
# executed.
send "$argv sql\r"
eexpect root@
send "drop database if exists t; create database t; create table t.foo(x int);\r"
eexpect "CREATE TABLE"
eexpect root@
send "insert into t.foo(x) values (42)\r"
eexpect " ->"
send "\004"
eexpect ":/# "

send "$argv sql\r"
eexpect root@
send "select * from t.foo;\r"
eexpect "0 rows"
eexpect root@
send "\\q\r"
eexpect ":/# "

# Check that an incomplete last statement in non-interactive mode
# is not executed, and fails with a warning. #8838
send "echo 'insert into t.foo(x) values (42)' | $argv sql\r"
eexpect "missing semicolon at end of statement"
eexpect ":/# "

# Check that a final comment after a missing semicolon and without
# newline is properly ignored when reasoning about the last
# statement. #9482
send "echo 'insert into t.foo(x) values (42)--;' | $argv sql\r"
eexpect "missing semicolon at end of statement"
eexpect ":/# "

send "echo 'select * from t.foo;' | $argv sql\r"
eexpect "0 rows"
eexpect ":/# "

# Check that a complete last statement terminated with a semicolon
# just before EOF and without a newline is properly executed. #7328
send "printf 'insert into t.foo(x) values(42);' | $argv sql\r"
eexpect "INSERT"
eexpect ":/# "
send "echo 'select * from t.foo;' | $argv sql\r"
eexpect "1 row"
eexpect ":/# "

# Check that a final comment after a final statement does not cause an
# error message. #9482
send "printf 'select 1;-- final comment' | $argv sql\r"
eexpect "1 row\r\n1\r\n1\r\n:/# "

# Check that a final comment does not cause an error message. #9243
send "printf 'select 1;\\n-- final comment' | $argv sql\r"
eexpect "1 row\r\n1\r\n1\r\n:/# "

# Finally terminate with Ctrl+C
send "exit\r"
eexpect eof

stop_server $argv
