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
# Note: we really would like to check for redaction markers here, alas this grep
# command is running inside the acceptance image and does not know about UTF-8.
# So we use an imprecise match instead.
system "grep -q 'sensitive_table_access.*ALTER TABLE.*helloworld.*\"TableName\":\".*t.public.helloworld.*\",\"AccessMode\":\"rw\"' $logfile"
send "SELECT * FROM helloworld;\r"
eexpect root@
system "grep -q 'sensitive_table_access.*SELECT.*helloworld.*\"TableName\":\".*t.public.helloworld.*\",\"AccessMode\":\"r\"' $logfile"
end_test

start_test "Check that write statements are logged differently"
send "INSERT INTO helloworld VALUES(456);\r"
eexpect root@
system "grep -q 'sensitive_table_access.*INSERT.*helloworld.*AccessMode\":\"rw\"' $logfile"
end_test

start_test "Check that errors get logged too"
send "SELECT nonexistent FROM helloworld;\r"
eexpect root@
system "grep -q 'sensitive_table_access.*SELECT.*nonexistent.*SQLSTATE.*42703.*\"AccessMode\":\"r\"' $logfile"
end_test

# Flush and truncate the logs. The test below must not see the log entries that
# were already generated above.
flush_server_logs
system "truncate -s0 $logfile"

# Check the log indeed is empty
system "if grep -q helloworld $logfile; then false; fi"

start_test "Check that audit removal is logged too"
send "ALTER TABLE helloworld EXPERIMENTAL_AUDIT SET OFF;\r"
eexpect root@
system "grep -q 'sensitive_table_access.*ALTER TABLE.*helloworld.*SET OFF.*AccessMode\":\"rw\"' $logfile"
end_test

send_eof
eexpect eof

stop_server $argv

start_test "Check that audit logging works even with a custom directory"
# Start a server with a custom log
system "$argv start-single-node --insecure --pid-file=server_pid --background -s=path=logs/db --sql-audit-dir=logs/db/audit-new >>logs/expect-cmd.log 2>&1;
        $argv sql --insecure -e 'select 1'"

set logfile logs/db/audit-new/cockroach-sql-audit.log

# Start a client and make a simple audit test.
spawn $argv sql
eexpect root@
send "create database d; create table d.helloworld(x INT);\r"
eexpect CREATE
eexpect root@
send "alter table d.helloworld EXPERIMENTAL_AUDIT SET READ WRITE;\r"
eexpect "ALTER TABLE"
eexpect root@
send "select x from d.helloworld;\r"
eexpect root@
send_eof
eexpect eof

# Check the file was created and populated properly.
system "grep -q helloworld $logfile"

stop_server $argv
end_test
