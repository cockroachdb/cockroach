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
send "SET CLUSTER SETTING sql.trace.log_statement_execute = TRUE;\r"
eexpect root@
system "test -e $logfile"
end_test

start_test "Check that statements get logged to the exec log but stop logging when disabled"
send "SELECT 'helloworld';\r"
eexpect root@

# Errors must be logged too
send "SELECT nonexistent;\r"
eexpect "column name"
eexpect "nonexistent"
eexpect "not found"
eexpect root@

# Check logging after disable
send "SET CLUSTER SETTING sql.trace.log_statement_execute = FALSE;\r"
eexpect root@
send "SELECT 'lovely';\r"
eexpect root@

flush_server_logs

# Now check the items are there in the log file.
system "grep -q helloworld $logfile"
system "grep -q nonexistent $logfile"
system "if grep -q lovely $logfile; then false; fi"

end_test

stop_server $argv
