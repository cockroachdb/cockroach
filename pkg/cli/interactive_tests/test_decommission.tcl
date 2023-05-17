#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check that a we can create a simple 3-node cluster"

system "$argv start --insecure --port=26257 --max-sql-memory=128MB --http-port=8082 --pid-file=server_pid1 --background -s=path=logs/db1 --join=:26257 >>logs/expect-cmd.log 2>&1"

system "$argv init --insecure --port=26257 >>logs/expect-cmd.log 2>&1;
        $argv sql -e 'select 1' --port=26257"

system "$argv start --insecure --port=26258 --max-sql-memory=128MB --http-port=8083 --pid-file=server_pid2 --background -s=path=logs/db2 --join=:26257 >>logs/expect-cmd.log 2>&1;
        $argv sql -e 'select 1' --port=26258"

system "$argv start --insecure --port=26259 --max-sql-memory=128MB --http-port=8084 --pid-file=server_pid3 --background -s=path=logs/db3 --join=:26257 >>logs/expect-cmd.log 2>&1;
        $argv sql -e 'select 1' --port=26259"

# Check the number of nodes
spawn $argv node ls
eexpect id
eexpect "3 rows"
eexpect eof

end_test

start_test "Check that we reach RF=3"

spawn $argv sql --port=26257 --no-line-editor
eexpect "defaultdb>"

send "SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='2GiB';\r"
eexpect "SET CLUSTER SETTING"

send "SET CLUSTER SETTING server.time_until_store_dead='25s';\r"
eexpect "SET CLUSTER SETTING"

set replicated3x 0

# This tries for up to 300 seconds as upreplication can take some time.
for {set i 0} {$i < 30} {incr i} {
  set timeout 1
  send "SELECT min(array_length(replicas, 1)) rf FROM \[SHOW CLUSTER RANGES\];\r"
  eexpect "rf"
  eexpect "?-*-\r\n"
  expect {
    "  3" {
      set replicated3x 1
      break
    }
    timeout {}
  }
  sleep 10
}

if {!$replicated3x} {
  report "Not replicated 3x"
  exit 1
}
eexpect "defaultdb>"

send_eof
eexpect eof
end_test

start_test "Check that decommissioning below RF=3 is not allowed"
set timeout 30

spawn $argv node decommission 1 --wait none
eexpect "ranges blocking decommission detected"
eexpect "ERROR: Cannot decommission nodes."
eexpect eof
expect_exit_status 1
end_test

start_test "Add a 4th node"

system "$argv start --insecure --port=26260 --max-sql-memory=128MB --http-port=8085 --pid-file=server_pid4 --background -s=path=logs/db4 --join=:26257 >>logs/expect-cmd.log 2>&1;
        $argv sql -e 'select 1' --port=26260"

# Check the number of nodes
spawn $argv node ls
eexpect id
eexpect "4 rows"
eexpect eof

end_test

start_test "Check that we can decommission safely at RF=3"

# Give the node time to complete decommissioning.
set timeout 300
spawn $argv node decommission 3
eexpect "| decommissioning |    false    |   ready   |"
eexpect eof
expect_exit_status 0
end_test

start_test "Check that subsequent decommission attempts do not error"

sleep 30
set timeout 30
spawn $argv node decommission 3
eexpect "| decommissioned |    false    | already decommissioned |"
eexpect eof
expect_exit_status 0

end_test

# Kill the cluster. We don't care about what happens next in this test,
# and this makes the test complete faster.
system "kill -KILL `cat server_pid1` `cat server_pid2` `cat server_pid3` `cat server_pid4`"
