#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

start_test "Check that it is possible to add nodes to a server started with start-single-node"

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

start_test "Check that a recommissioning an active node prints out a warning"
spawn $argv node recommission 2
eexpect "warning: node 2 is not decommissioned"
eexpect eof

start_test "Check that a double decommission prints out a warning"
spawn $argv node decommission 2 --wait none
eexpect eof

spawn $argv node decommission 2 --wait none
eexpect "warning: node 2 is already decommissioning or decommissioned"
eexpect eof
end_test

end_test


# Kill the cluster. We don't care about what happens next in this test,
# and this makes the test complete faster.
system "kill -KILL `cat server_pid` `cat server_pid2` `cat server_pid3`"
