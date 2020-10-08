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
send "SET CLUSTER   SETTING sql.trace.log_statement_execute = TRUE;\r"
eexpect "SET CLUSTER SETTING"
eexpect root@
system "test -e $logfile"
end_test

start_test "Check that statements get logged to the exec log but stop logging when disabled"
send "SELECT 'hello' || 'world';\r"
eexpect "helloworld"
eexpect root@

# Errors must be logged too
send "SELECT nonexistent;\r"
eexpect "column"
eexpect "nonexistent"
eexpect "does not exist"
eexpect root@

# Check logging after disable
send "SET CLUSTER SETTING sql.trace.log_statement_execute = FALSE;\r"
eexpect root@
send "SELECT 'lov' || 'ely';\r"
eexpect "lovely"
eexpect root@

flush_server_logs

# Now check the items are there in the log file.
# We need to iterate because flush_server_logs
# only syncs on flush of cockroach.log, not
# the exec log.
system "for i in `seq 1 3`; do
  grep 'hello.*world' $logfile &&
  grep nonexistent $logfile &&
  exit 0;
  echo still waiting;
  sleep 1;
done;
echo 'server failed to flush exec log?'
exit 1;"
system "if grep -q lovely $logfile; then false; fi"

end_test

stop_server $argv
