#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

start_test "Check that it is possible to add nodes to a server started with start-single-node"

system "$argv start --insecure --port=26258 --http-port=8083 --pid-file=server_pid2 --background -s=path=logs/db2 --join=:26257 >>logs/expect-cmd.log 2>&1;
        $argv sql -e 'select 1' --port=26258"

system "$argv start --insecure --port=26259 --http-port=8084 --pid-file=server_pid3 --background -s=path=logs/db3 --join=:26257 >>logs/expect-cmd.log 2>&1;
        $argv sql -e 'select 1' --port=26259"

# Check the number of nodes
spawn $argv node ls
eexpect id
eexpect "3 rows"
eexpect eof

# Remove the additional nodes.
system "$argv quit --port=26258"
system "$argv quit --port=26259"

end_test

stop_server $argv
