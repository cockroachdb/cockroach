#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that demo disables replication properly"
send "$argv demo -e 'show zone configuration for range default'\r"
eexpect "num_replicas = 1"
eexpect ":/# "
end_test

start_test "Check that start-single-node disables replication properly"
system "rm -rf logs/db"
start_server $argv
send "$argv sql -e 'show zone configuration for range default'\r"
eexpect "num_replicas = 1"
eexpect ":/# "
end_test

start_test "Check that it remains possible to reset the replication factor"
send "$argv sql -e 'alter range default configure zone using num_replicas = 3'\r"
eexpect "CONFIGURE ZONE"
eexpect ":/# "
stop_server $argv
start_server $argv
send "$argv sql -e 'show zone configuration for range default'\r"
eexpect "num_replicas = 3"
eexpect ":/# "
end_test

stop_server $argv

start_test "Check that start-single-node on a regular cluster does not reset the replication factor"
# make a fresh server but using the regular 'start'
system "rm -rf logs/db"
system "$argv start --insecure --pid-file=server_pid --background -s=path=logs/db --join=:26257 >>logs/expect-cmd.log 2>&1"
system "$argv init --insecure"
system "$argv sql -e 'select 1'"
# restart with start-single-node
stop_server $argv
start_server $argv
# check that the replication factor was unchanged
send "$argv sql -e 'show zone configuration for range default'\r"
eexpect "num_replicas = 3"
eexpect ":/# "
end_test

send "exit 0\r"
eexpect eof
