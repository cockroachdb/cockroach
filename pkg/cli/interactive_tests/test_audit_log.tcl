#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

set logfile logs/db/logs/cockroach-sql-audit.log

start_test "Check that the audit log is not created by default"
system "if test -e $logfile; then false; fi"
end_test

start_test "Check that statements do not get logged to the audit log directly"
send "CREATE DATABASE t; USE t; CREATE TABLE helloworld(abc INT); INSERT INTO helloworld VALUES (123);\r"
eexpect root@
system "if test -e $logfile; then false; fi"
end_test

start_test "Check that statements start being logged synchronously if auditing is enabled"
send "ALTER TABLE helloworld EXPERIMENTAL_AUDIT SET READ WRITE;\r"
eexpect root@
# check that the audit change itself is recorded.
system "grep -q 'helloworld.*:READWRITE.*ALTER TABLE.*OK' $logfile"
send "SELECT * FROM helloworld;\r"
eexpect root@
system "grep -q 'helloworld.*:READ}.*SELECT.*OK' $logfile"
end_test

start_test "Check that write statements are logged differently"
send "INSERT INTO helloworld VALUES(456);\r"
eexpect root@
system "grep -q 'helloworld.*:READWRITE.*INSERT.*OK' $logfile"
end_test

start_test "Check that errors get logged too"
send "SELECT nonexistent FROM helloworld;\r"
eexpect root@
system "grep -q 'helloworld.*:READ}.*SELECT.*ERROR' $logfile"
end_test

# force rotate the logs: the test below must not see the
# log entries that were already generated above.
force_stop_server $argv
start_server $argv

send "\r"
eexpect "opening new connection"
eexpect root@
send "\r"
eexpect "t>"

start_test "Check that audit removal is logged too"
send "ALTER TABLE helloworld EXPERIMENTAL_AUDIT SET OFF;\r"
eexpect root@
system "grep 'helloworld.*:READWRITE.*ALTER TABLE.*SET OFF.*OK' $logfile"
end_test

interrupt
eexpect eof

stop_server $argv
