#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set pid [spawn $argv sql]
eexpect root@

# Check that an error in the last statement is propagated to the shell.
send "select ++;\r"
eexpect "syntax error"
eexpect root@
send "\\q\r"

set ret [wait]
set status [lindex $ret 3]
if { $status == 0 } {
    send_error "error during last statement does not propagate to the exit code"
}

# Check that an incomplete last statement in interactive mode is not
# executed.
spawn $argv sql
eexpect root@
send "drop database if exists t; create database t; create table t.foo(x int);\r"
eexpect "CREATE TABLE"
eexpect root@
send "insert into t.foo(x) values (42)\r"
eexpect " ->"
send "\004"
eexpect eof

spawn $argv sql
eexpect root@
send "select * from t.foo;\r"
eexpect "0 rows"
eexpect root@
send "\\q\r"
eexpect eof

spawn /bin/bash
eexpect ":/# "

# Check that an incomplete last statement in non-interactive mode
# is not executed, and fails with a warning. #8838
send "echo 'insert into t.foo(x) values (42)' | $argv sql\r"
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

# Finally terminate with Ctrl+C
send "exit\r"
eexpect eof
