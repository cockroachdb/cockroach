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

# Re-enable logging for the next test.
send "SET CLUSTER   SETTING sql.trace.log_statement_execute = TRUE;\r"
eexpect "SET CLUSTER SETTING"
eexpect root@

start_test "Check that a txn counter is added in the 2nd column after the status"

# Two statements inside the same txn that will want the same txn counter value.
send "BEGIN; SELECT 770+7; SELECT 770+7; COMMIT;\r"
eexpect 777
eexpect 777
eexpect root@

# Two statements separated by an auto-retry, to verify the txn counter
# does not get trashed by retries.
send "BEGIN; SELECT 880+8; SELECT crdb_internal.force_retry('2ms'::INTERVAL); SELECT 880+8; COMMIT;\r"
eexpect 888
eexpect 888
eexpect root@

# Two statements separated by a manual retry.
send "BEGIN; SAVEPOINT cockroach_restart; SELECT 990+9; ROLLBACK TO SAVEPOINT cockroach_restart; SELECT 990+9; COMMIT;\r"
eexpect 999
eexpect 999
eexpect root@

# Two standalone statements that will want separate counters.
send "SELECT 660+6; SELECT 660+6;\r"
eexpect 666
eexpect 666
eexpect root@

flush_server_logs

# Now check the items are there in the log file. We need to iterate
# because flush_server_logs only syncs on flush of cockroach.log, not
# the exec log.
#
# We also check the last statement first, this ensures that every
# previous statement is also in the log file after this check
# succeeds.
system "for i in `seq 1 3`; do
  grep 'SELECT 660' $logfile && exit 0;
  echo still waiting;
  sleep 1;
done;
echo 'not finding two separate txn counter values?';
grep 'SELECT 660' $logfile;
exit 1;"

# Two separate single-stmt txns.
system "n=`grep 'SELECT 660' $logfile | sed -e 's/.*TxnCounter.:\\(\[0-9\]*\\)/\\1/g' | uniq | wc -l`; if test \$n -ne 2; then echo unexpected \$n; exit 1; fi"
# Same txns.
system "n=`grep 'SELECT 770' $logfile | sed -e 's/.*TxnCounter.:\\(\[0-9\]*\\)/\\1/g' | uniq | wc -l`; if test \$n -ne 1; then echo unexpected \$n; exit 1; fi"
system "n=`grep 'SELECT 880' $logfile | sed -e 's/.*TxnCounter.:\\(\[0-9\]*\\)/\\1/g' | uniq | wc -l`; if test \$n -ne 1; then echo unexpected \$n; exit 1; fi"
system "n=`grep 'SELECT 990' $logfile | sed -e 's/.*TxnCounter.:\\(\[0-9\]*\\)/\\1/g' | uniq | wc -l`; if test \$n -ne 1; then echo unexpected \$n; exit 1; fi"

end_test

stop_server $argv
