#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

set logfile logs/db/logs/cockroach-sql-exec.log

start_test "Check that the exec log is not created by default"
system "if test -e $logfile; then false; fi"
end_test

start_test "Check that the exec log is created after enabled"
send "SET CLUSTER SETTING sql.execlog.enabled = TRUE;\r"
eexpect root@
system "test -e $logfile"
end_test

start_test "Check that statements get logged to the exec log synchronously"
send "SELECT 'helloworld';\r"
eexpect root@
system "grep -q helloworld $logfile"
end_test

start_test "Check that errors get logged too"
send "SELECT nonexistent;\r"
eexpect "column name"
eexpect "nonexistent"
eexpect "not found"
eexpect root@
system "grep -q nonexistent $logfile"
end_test

start_test "Check that logging can be disabled"
send "SET CLUSTER SETTING sql.execlog.enabled = FALSE;\r"
eexpect root@
send "SELECT 'lovely';\r"
eexpect root@
system "if grep -q lovely $logfile; then false; fi"
end_test

interrupt
eexpect eof

stop_server $argv
