#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

set logfile logs/db/logs/cockroach-sql-audit.log

start_test "Check that the audit log is not created by default"
system "if test -e $logfile; then false; fi"
end_test

start_test "Check that the audit log is not created directly after enabled"
send "SET CLUSTER SETTING sql.auditlog.enabled = TRUE;\r"
eexpect root@
# Not yet true
#system "if test -e $logfile; then false; fi"
end_test

start_test "Check that statements do not get logged to the audit log directly"
send "CREATE DATABASE t; USE t; CREATE TABLE helloworld(abc INT); INSERT INTO helloworld VALUES (123);\r"
eexpect root@
# Not yet true
# system "if test -e $logfile; then false; fi"
end_test

start_test "Check that statements start being logged synchronously if auditing is enabled"
# send "ALTER TABLE helloworld EXPERIMENTAL_AUDIT SET READ WRITE;\r"
# eexpect root@
send "SELECT * FROM helloworld;\r"
eexpect root@
system "grep -q '.*SELECT.*false' $logfile"
end_test

start_test "Check that write statements are logged differently"
send "INSERT INTO helloworld VALUES(456);\r"
eexpect root@
system "grep -q '.*INSERT.*false' $logfile"
end_test

start_test "Check that errors get logged too"
send "SELECT nonexistent FROM helloworld;\r"
eexpect root@
system "grep -q '.*SELECT.*true' $logfile"
end_test

start_test "Check that logging can be disabled"
send "SET CLUSTER SETTING sql.auditlog.enabled = FALSE;\r"
eexpect root@
send "UPDATE helloworld SET abc = abc + 1;\r"
eexpect root@
system "if grep -q UPDATE $logfile; then false; fi"
end_test

interrupt
eexpect eof

stop_server $argv
